#pragma once

#include <zk/client.hpp>
#include <zk/error.hpp>
#include <zk/multi.hpp>
#include <zk/types.hpp>


#include "client_interface.hpp"
#include "time_machine.hpp"
#include "util.hpp"
#include "key.hpp"



namespace liboffkv {

class ZKClient : public Client {
private:
    using buffer = zk::buffer;

    using Key = key::Key;

    zk::client client_;

    static
    buffer from_string(const std::string& str)
    {
        return {str.begin(), str.end()};
    }

    static
    std::string to_string(const buffer& buf)
    {
        return {buf.begin(), buf.end()};
    }

    void make_recursive_erase_query(zk::multi_op& query, const std::string& path, bool ignore_no_entry = true)
    {
        zk::get_children_result::children_list_type children;
        bool entry_valid = true;
        try {
            children = client_.get_children(path).get().children();
        } catch (zk::no_entry& err) {
            if (!ignore_no_entry)
                throw err;
            entry_valid = false;
        }

        if (entry_valid) {
            for (const auto& child : children)
                make_recursive_erase_query(query, path + "/" + child, true);

            query.push_back(zk::op::erase(path));
        }
    }

    const Key get_path_(const std::string& key) const
    {
        Key res = key;
        res.set_prefix(prefix_);
        return res;
    }

    std::future<TransactionResult> commit_impl_(Transaction transaction)
    {
        auto boundaries = std::make_shared<std::vector<size_t>>();

        std::future<zk::multi_op> txn = thread_pool_->async(
            [this, transaction = std::move(transaction), boundaries_ptr = boundaries] {
                zk::multi_op trn;
                std::vector<size_t>& boundaries = *boundaries_ptr;

                for (const auto& check : transaction.checks()) {
                    trn.push_back(
                        zk::op::check(static_cast<std::string>(get_path_(check.key)),
                                      check.version ? zk::version(check.version - 1) : zk::version::any()));
                    boundaries.emplace_back(trn.size() - 1);
                }

                for (const auto& op_ptr : transaction.operations()) {
                    switch (op_ptr->type) {
                        case op::op_type::CREATE: {
                            auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());
                            trn.push_back(zk::op::create(
                                get_path_(create_op_ptr->key),
                                from_string(create_op_ptr->value),
                                (!create_op_ptr->leased ? zk::create_mode::normal : zk::create_mode::ephemeral)
                            ));
                            break;
                        }
                        case op::op_type::SET: {
                            auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());
                            trn.push_back(zk::op::set(static_cast<std::string>(get_path_(set_op_ptr->key)),
                                                      from_string(set_op_ptr->value)));
                            break;
                        }
                        case op::op_type::ERASE: {
                            auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());
                            make_recursive_erase_query(trn, get_path_(erase_op_ptr->key));
                            break;
                        }
                    }
                    boundaries.emplace_back(trn.size() - 1);
                }

                return trn;
            });

        return thread_pool_->then(
            thread_pool_->then(
                std::move(txn),
                [this](std::future<zk::multi_op>&& txn) {
                    return client_.commit(txn.get()).get();
                },
                true
            ), [boundaries_ptr = boundaries](std::future<zk::multi_result>&& multi_res) {
                TransactionResult result;

                try {
                    auto multi_res_unwrapped = util::call_get(std::move(multi_res));

                    for (const auto& res : multi_res_unwrapped) {
                        switch (res.type()) {
                            case zk::op_type::set:
                                result.push_back(SetResult{
                                    static_cast<uint64_t>(res.as_set().stat().data_version.value + 1)
                                });
                                break;
                            case zk::op_type::create:
                                result.push_back(CreateResult{1});
                                break;
                            case zk::op_type::check:
                            case zk::op_type::erase:
                                throw 1;
                        }
                    }
                } catch (zk::transaction_failed& e) {
                    const auto op_index = e.failed_op_index();

                    const std::vector<size_t>& boundaries = *boundaries_ptr;
                    const auto user_op_index = util::user_op_index(boundaries, op_index);
                    if (boundaries[user_op_index] != op_index)
                        std::rethrow_exception(std::current_exception());
                    throw TransactionFailed{user_op_index};
                }

                return result;
            }
        );
    }

public:
    ZKClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> time_machine)
        : Client(address, prefix, std::move(time_machine)),
          client_(zk::client::connect(address).get())
    {
        if (!prefix.empty()) {
            const std::vector<std::string> entries = key::get_entry_sequence(prefix);
            for (const auto& e : entries) {
                // start all operations asynchronously
                // because ZK guarantees sequential consistency
                client_.create(e, buffer());
            }
        }
    }

    ZKClient() = delete;

    ZKClient(const ZKClient&) = delete;

    ZKClient& operator=(const ZKClient&) = delete;


    ~ZKClient() override = default;


    std::future<CreateResult> create(const std::string& key, const std::string& value, bool lease = false) override
    {
        return thread_pool_->then(
            client_.create(
                static_cast<std::string>(get_path_(key)),
                from_string(value),
                !lease ? zk::create_mode::normal : zk::create_mode::ephemeral
            ),
            [](std::future<zk::create_result>&& res) -> CreateResult {
                util::call_get_ignore(std::move(res));
                return {1};
            }
        );
    }

    std::future<ExistsResult> exists(const std::string& key, bool watch = false) override
    {
        if (watch)
            return thread_pool_->then(
                client_.watch_exists(static_cast<std::string>(get_path_(key))),
                [tp = thread_pool_](std::future<zk::watch_exists_result>&& result) -> ExistsResult {
                    zk::watch_exists_result unwrapped = util::call_get(std::move(result));
                    auto stat = unwrapped.initial().stat();

                    return {
                        stat.has_value() ? static_cast<uint64_t>(stat.value().data_version.value + 1) : 0,
                        !!unwrapped.initial(),
                        tp->then(std::move(unwrapped.next()), util::call_get_ignore<zk::event>).share()
                    };
                });

        return thread_pool_->then(
            client_.exists(static_cast<std::string>(get_path_(key))),
            [](std::future<zk::exists_result>&& result) -> ExistsResult {
                zk::exists_result unwrapped = util::call_get(std::move(result));
                auto stat = unwrapped.stat();

                return {stat.has_value()
                        ? static_cast<uint64_t>(stat.value().data_version.value + 1) : 0,
                        !!unwrapped};
            });
    }


    std::future<ChildrenResult> get_children(const std::string& key, bool watch) override
    {
        if (watch)
            return thread_pool_->then(
                client_.watch_children(static_cast<std::string>(get_path_(key))),
                [this, key](std::future<zk::watch_children_result>&& result) -> ChildrenResult {
                    zk::watch_children_result unwrapped = util::call_get(std::move(result));
                    const std::vector<std::string>& raw_children = unwrapped.initial().children();
                    return {
                        util::map_vector(raw_children, [this, key](const auto& child) { return key + "/" + child; }),
                        thread_pool_->then(std::move(unwrapped.next()), util::call_get_ignore<zk::event>)
                    };
                }
            );

        return thread_pool_->then(
            client_.get_children(static_cast<std::string>(get_path_(key))),
            [this, key](std::future<zk::get_children_result>&& result) -> ChildrenResult {
                zk::get_children_result unwrapped = util::call_get(std::move(result));
                const std::vector<std::string>& raw_children = unwrapped.children();
                return {
                    util::map_vector(raw_children, [this, key](const auto& child) { return key + "/" + child; })
                };
            }
        );
    }

    // No transactions. Atomicity is not necessary for linearizability here!
    // At least it seems to be so...
    std::future<SetResult> set(const std::string& key, const std::string& value) override
    {
        auto path = static_cast<std::string>(get_path_(key));

        return thread_pool_->then(
            client_.create(path, from_string(value)),
            [this, path, value](std::future<zk::create_result>&& res) -> SetResult {
                try {
                    util::call_get(std::move(res));
                    return {1};
                } catch (EntryExists&) {
                    return util::call_get(thread_pool_->then(
                        client_.set(path, from_string(value)),
                        [path](std::future<zk::set_result>&& result) -> SetResult {
                            try {
                                return {
                                    static_cast<uint64_t>(util::call_get(std::move(result)).stat().data_version.value +
                                                          1)};
                            } catch (NoEntry&) {
                                // concurrent remove happened
                                // but set must not throw NoEntry
                                // hm, we don't know real version
                                // doesn't matter, return some large number instead
                                return {static_cast<uint64_t>(1) << 63};
                            }
                        },
                        true
                    ));
                }
            }
        );
    }

    // Same as set: transactions aren't necessary
    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) override
    {
        auto path = static_cast<std::string>(get_path_(key));

        if (!version) {
            return thread_pool_->then(
                create(key, value),
                [this, path, value](std::future<CreateResult>&& res) -> CASResult {
                    try {
                        util::call_get(std::move(res));
                        return {1, true};
                    } catch (EntryExists&) {
                        return util::call_get(thread_pool_->then(
                            client_.get(path),
                            [](std::future<zk::get_result>&& result) -> CASResult {
                                try {
                                    return {
                                        static_cast<uint64_t>(
                                            util::call_get(std::move(result)).stat().data_version.value +
                                            1),
                                        false
                                    };
                                } catch (NoEntry&) {
                                    // concurrent remove happened
                                    // cas with zero version must not throw NoEntry
                                    return {
                                        static_cast<uint64_t>(1) << 63,
                                        false
                                    };
                                }
                            },
                            true
                        ));
                    }
                });
        }

        return thread_pool_->then(
            thread_pool_->then(
                client_.set(static_cast<std::string>(get_path_(key)), from_string(value), zk::version(version - 1)),
                [this, version, path](std::future<zk::set_result>&& result) -> CASResult {
                    try {
                        return {static_cast<uint64_t>(result.get().stat().data_version.value + 1), true};
                    } catch (zk::error& e) {
                        if (e.code() == zk::error_code::no_entry)
                            throw NoEntry{};

                        if (e.code() == zk::error_code::version_mismatch) {
                            return thread_pool_->then(
                                client_.get(path),
                                [version](std::future<zk::get_result>&& result) -> CASResult {
                                    try {
                                        return {
                                            static_cast<uint64_t>(
                                                util::call_get(std::move(result)).stat().data_version.value + 1),
                                            false
                                        };
                                    } catch (zk::error& e) {
                                        if (e.code() == zk::error_code::no_entry) {
                                            return {
                                                static_cast<uint64_t>(1) << 63,
                                                false
                                            };
                                        }
                                        throw e;
                                    }
                                },
                                true
                            ).get();
                        }
                        throw e;
                    }
                }
            ),
            util::call_get<CASResult>
        );
    }

    std::future<GetResult> get(const std::string& key, bool watch = false) override
    {
        if (watch) {
            return thread_pool_->then(
                client_.watch(static_cast<std::string>(get_path_(key))),
                [tp = thread_pool_](std::future<zk::watch_result>&& result) -> GetResult {
                    auto full_res = util::call_get(std::move(result));
                    auto res = full_res.initial();
                    return {
                        static_cast<uint64_t>(res.stat().data_version.value + 1),
                        to_string(res.data()),
                        tp->then(std::move(full_res.next()), util::call_get_ignore<zk::event>).share()
                    };
                }
            );
        }

        return thread_pool_->then(
            client_.get(static_cast<std::string>(get_path_(key))),
            [](std::future<zk::get_result>&& result) -> GetResult {
                auto res = util::call_get(std::move(result));
                return {static_cast<uint64_t>(res.stat().data_version.value + 1), to_string(res.data())};
            }
        );
    }

    std::future<void> erase(const std::string& key, uint64_t version = 0) override
    {
        return thread_pool_->then(
            thread_pool_->async([this, key, version] {
                while (true) {
                    auto path = static_cast<std::string>(get_path_(key));

                    zk::multi_op txn;
                    txn.push_back(zk::op::check(path));
                    txn.push_back(zk::op::check(path, version ? zk::version(version - 1) : zk::version::any()));

                    make_recursive_erase_query(txn, static_cast<std::string>(get_path_(key)), false);

                    try {
                        auto res = client_.commit(txn).get();
                        return;
                    } catch (zk::transaction_failed& e) {
                        if (e.failed_op_index() == 0)
                            throw NoEntry{};
                        if (e.failed_op_index() == 1)
                            return;
                    }
                }
            }),
            util::call_get<void>,
            true
        );
    }

    std::future<TransactionResult> commit(const Transaction& transaction) override
    {
        auto promise = std::make_shared<std::promise<TransactionResult>>();
        auto try_commit = std::make_shared<std::function<void(void)>>();

        *try_commit = [this, try_commit, transaction, promise] {
            thread_pool_->then(
                commit_impl_(transaction),
                [try_commit, promise](std::future<TransactionResult>&& res) -> void {
                    try {
                        promise->set_value(res.get());
                    } catch (zk::transaction_failed& e) {
                        (*try_commit)();
                    } catch (...) {
                        promise->set_exception(std::current_exception());
                    }
                }
            );
        };

        (*try_commit)();
        return promise->get_future();
    }
};

} // namespace liboffkv
