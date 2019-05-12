#pragma once

#include <zk/client.hpp>
#include <zk/error.hpp>
#include <zk/multi.hpp>
#include <zk/types.hpp>


#include "client_interface.hpp"
#include "time_machine.hpp"
#include "util.hpp"
#include "key.hpp"



class ZKClient : public Client {
private:
    using buffer = zk::buffer;

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

    std::string get_path(const std::string& key) const
    {
        return prefix_ + key;
    }

    zk::client client_;
    std::string prefix_;

public:
    ZKClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> time_machine)
        : Client(address, std::move(time_machine)),
          client_(zk::client::connect(address).get()),
          prefix_(prefix)
    {
        if (!prefix.empty()) {
            const std::vector<std::string> entries = get_entry_sequence(prefix);
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


    std::future<void> create(const std::string& key, const std::string& value, bool lease = false) override
    {
        return thread_pool_->then(
            client_.create(
                get_path(key),
                from_string(value),
                !lease ? zk::create_mode::normal : zk::create_mode::ephemeral
            ),
            call_get_ignore<zk::create_result>
        );
    }

    std::future<ExistsResult> exists(const std::string& key) const override
    {
        return thread_pool_->then(
            client_.exists(get_path(key)),
            [](std::future<zk::exists_result>&& result) -> ExistsResult {
                zk::exists_result unwrapped = call_get(std::move(result));
                auto stat = unwrapped.stat();

                return {stat.has_value()
                        ? static_cast<uint64_t>(stat.value().data_version.value + 1) : 0,
                        !!unwrapped};
            });
    }


    // No transactions. Atomicity is not necessary for linearizability here!
    // At least it seems to be so...
    std::future<SetResult> set(const std::string& key, const std::string& value) override
    {
        auto path = get_path(key);

        return thread_pool_->then(
            client_.create(path, from_string(value)),
            [this, path, value](std::future<zk::create_result>&& res) -> SetResult {
                try {
                    call_get(std::move(res));
                    return {1};
                } catch (EntryExists&) {
                    return call_get(thread_pool_->then(
                        client_.set(path, from_string(value)),
                        [path](std::future<zk::set_result>&& result) -> SetResult {
                            try {
                                return {static_cast<uint64_t>(call_get(std::move(result)).stat().data_version.value + 1)};
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
        auto path = get_path(key);

        if (!version) {
            return thread_pool_->then(
                create(key, value),
                [this, path, value](std::future<void>&& res) -> CASResult {
                    try {
                        call_get(std::move(res));
                        return {1, true};
                    } catch (EntryExists&) {
                        return call_get(thread_pool_->then(
                            client_.get(path),
                            [](std::future<zk::get_result>&& result) -> CASResult {
                                try {
                                    return {
                                        static_cast<uint64_t>(call_get(std::move(result)).stat().data_version.value + 1),
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
                client_.set(get_path(key), from_string(value), zk::version(version - 1)),
                [this, version, path](std::future<zk::set_result>&& result) -> CASResult {
                    try {
                        return {static_cast<uint64_t>(result.get().stat().data_version.value + 1), true};
                    } catch (zk::error& e) {
                        if (e.code() == zk::error_code::no_entry) {
                            return {0, false};
                        }
                        if (e.code() == zk::error_code::version_mismatch) {
                            return thread_pool_->then(
                                client_.get(path),
                                [version](std::future<zk::get_result>&& result) -> CASResult {
                                    try {
                                        return {
                                            static_cast<uint64_t>(call_get(std::move(result)).stat().data_version.value + 1),
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
            call_get<CASResult>
        );
    }

    std::future<GetResult> get(const std::string& key, bool watch = false) const override
    {
        if (watch) {
            return thread_pool_->then(
                client_.watch(get_path(key)),
                [tp = thread_pool_](std::future<zk::watch_result>&& result) -> GetResult {
                    auto full_res = call_get(std::move(result));
                    auto res = full_res.initial();
                    return {
                        static_cast<uint64_t>(res.stat().data_version.value + 1),
                        to_string(res.data()),
                        std::make_unique<std::future<void>>(tp->then(std::move(full_res.next()), call_get_ignore<zk::event>))
                    };
                }
            );
        }

        return thread_pool_->then(
            client_.get(get_path(key)),
            [](std::future<zk::get_result>&& result) -> GetResult {
                auto res = call_get(std::move(result));
                return {static_cast<uint64_t>(res.stat().data_version.value + 1), to_string(res.data())};
            }
        );
    }

    std::future<void> erase(const std::string& key, uint64_t version = 0) override
    {
        auto path = get_path(key);

        if (version == uint64_t(0))
            return thread_pool_->then(client_.erase(path), call_get_ignore_noexcept<void>);
        return thread_pool_->then(client_.erase(path, zk::version(version - 1)), call_get_ignore_noexcept<void>);
    }

    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        zk::multi_op trn;

        for (const auto& check : transaction.checks())
            trn.push_back(zk::op::check(get_path(check.key), zk::version(check.version)));

        for (const auto& op_ptr : transaction.operations()) {
            switch (op_ptr->type) {
                case op::op_type::CREATE: {
                    auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());
                    trn.push_back(zk::op::create(get_path(create_op_ptr->key), from_string(create_op_ptr->value)));
                    break;
                }
                case op::op_type::SET: {
                    auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());
                    trn.push_back(zk::op::set(get_path(set_op_ptr->key), from_string(set_op_ptr->value)));
                    break;
                }
                case op::op_type::ERASE: {
                    auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());
                    trn.push_back(zk::op::erase(get_path(erase_op_ptr->key), zk::version(erase_op_ptr->version)));
                    break;
                }
                default: __builtin_unreachable();
            }
        };

        return thread_pool_->then(client_.commit(trn), [](std::future<zk::multi_result>&& multi_res) {
            TransactionResult result;

            auto multi_res_unwrapped = call_get(std::move(multi_res));

            for (const auto& res : multi_res_unwrapped) {
                switch (res.type()) {
                    case zk::op_type::create:
                        result.emplace_back(op::op_type::CREATE, std::make_shared<CreateResult>());
                        break;
                    case zk::op_type::set:
                        result.emplace_back(op::op_type::SET,
                                            std::make_shared<SetResult>(SetResult{
                                                static_cast<uint64_t>(res.as_set().stat().data_version.value)
                                            }));
                        break;
                    case zk::op_type::check:
                    case zk::op_type::erase:
                    default:
                        __builtin_unreachable();
                }
            }

            return result;
        });
    }
};
