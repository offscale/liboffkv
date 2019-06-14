#pragma once

#include <mutex>
#include "ppconsul/consul.h"
#include "ppconsul/kv.h"
#include "ppconsul/sessions.h"

#include "client_interface.hpp"
#include "key.hpp"


namespace liboffkv {

class ConsulClient : public Client {
private:
    static constexpr auto BLOCK_FOR_WHEN_WATCH = std::chrono::seconds(20);

    static constexpr auto TTL = std::chrono::seconds(10);

    static constexpr auto CONSISTENCY = ppconsul::Consistency::Consistent;

    using Kv = ppconsul::kv::Kv;
    using TxnOperation = ppconsul::kv::TxnOperation;
    using TxnAborted = ppconsul::kv::TxnAborted;
    using Consul = ppconsul::Consul;
    using Sessions = ppconsul::sessions::Sessions;

    using Key = key::Key;

    Consul client_;
    Kv kv_;
    Sessions sessions_;

    std::string prefix_;

    mutable std::mutex lock_;


    const Key get_path_(const std::string& key) const
    {
        Key res = key;
        res.set_prefix(prefix_);
        res.set_transformer([](const std::string &prefix, const std::vector<Key> &seq) {
            const std::string result = prefix + seq.rbegin()->get_raw_key();
            if (!result.empty() && result[0] == '/') {
                return result.substr(1);
            }
            return result;
        });
        return res;
    }

    const std::string detach_prefix_(const std::string& full_path) const
    {
        return full_path.substr(prefix_.size() - 1);
    }

    std::future<void> get_watch_future_(const std::string& key, uint64_t old_version) const
    {
        return std::async(std::launch::async, [this, key, old_version] {
            std::unique_lock lock(lock_);
            kv_.item(key, ppconsul::kv::kw::block_for = {BLOCK_FOR_WHEN_WATCH, old_version});
        });
    };

public:
    ConsulClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> time_machine)
        : Client(std::move(time_machine)),
          client_(address),
          kv_(client_, ppconsul::kw::consistency = CONSISTENCY),
          sessions_(client_, ppconsul::kw::consistency = CONSISTENCY),
          prefix_(prefix)
    {}

    ConsulClient() = delete;

    ConsulClient(const ConsulClient&) = delete;

    ConsulClient& operator=(const ConsulClient&) = delete;


    ~ConsulClient() = default;


    std::future<CreateResult> create(const std::string& key, const std::string& value, bool lease = false) override
    {
        using namespace ppconsul::kv;

        return this->thread_pool_->async(
            [this, key = get_path_(key), value, lease]() -> CreateResult {
                std::unique_lock lock(lock_);

                auto parent = key.get_parent();
                bool has_parent = (parent.size() > 0);

                try {
                    if (lease) {
                        auto id = sessions_.create(key, std::chrono::seconds{15},
                                                   ppconsul::sessions::InvalidationBehavior::Delete, TTL);
                        thread_pool_->periodic(
                            [this, id = std::move(id)] {
                                try {
                                    sessions_.renew(id);
                                    return (TTL + std::chrono::seconds(1)) / 2;
                                } catch (... /* BadStatus& ??*/) {
                                    return std::chrono::seconds::zero();
                                }
                            }, (TTL + std::chrono::seconds(1)) / 2);
                    }

                    std::vector<TxnOperation> ops;

                    if (has_parent)
                        ops.push_back(txn_ops::Get{parent});

                    ops.push_back(txn_ops::CheckNotExists{key});
                    ops.push_back(txn_ops::Set{key, value});

                    return {kv_.commit(ops).back().modifyIndex};

                } catch (TxnAborted& e) {
                    const auto opIndex = static_cast<size_t>(e.errors().front().opIndex);
                    if (has_parent && opIndex == 0)
                        throw NoEntry{};
                    if (opIndex == static_cast<size_t>(has_parent))
                        throw EntryExists{};
                    throw;
                } liboffkv_catch
            });
    }

    std::future<ExistsResult> exists(const std::string& key, bool watch = false) override
    {
        return this->thread_pool_->async(
            [this, key = get_path_(key), watch]() -> ExistsResult {
                std::unique_lock lock(lock_);
                try {
                    auto item = kv_.item(ppconsul::withHeaders, key);

                    std::future<void> watch_future;
                    if (watch)
                        watch_future = get_watch_future_(key, item.headers().index());

                    lock.unlock();

                    return {item.headers().index(), item.data().valid(), watch_future.share()};

                } liboffkv_catch
            });
    }

    std::future<ChildrenResult> get_children(const std::string& key, bool watch = false) override
    {
        using namespace ppconsul::kv;

        return this->thread_pool_->async(
            [this, key = get_path_(key), watch]() -> ChildrenResult {
                std::unique_lock lock(lock_);
                try {
                    auto res = kv_.commit({
                        txn_ops::GetAll{key},
                        txn_ops::Get{key},
                    });

                    std::future<void> watch_future;
                    if (watch)
                        watch_future = get_watch_future_(key, res.back().modifyIndex);

                    return {
                        util::map_vector(
                            util::filter_vector(
                                std::vector<KeyValue>(res.begin(), --res.end()),
                                [key = static_cast<std::string>(key)](const auto& key_value) {
                                    return key_value.key != key &&
                                           key_value.key.substr(key.size() + 1).find('/') == std::string::npos;
                                }),
                            [this](const auto& key_value) {
                                return detach_prefix_(key_value.key);
                            }),
                        watch_future.share()
                    };
                } catch (TxnAborted& e) {
                    throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value) override
    {
        using namespace ppconsul::kv;

        return thread_pool_->async(
            [this, key = get_path_(key), value]() -> SetResult {
                std::unique_lock lock(lock_);
                try {
                    auto parent = key.get_parent();

                    std::vector<TxnOperation> ops;

                    if (!parent.empty())
                        ops.push_back(txn_ops::Get{parent});

                    ops.push_back(txn_ops::Set{key, value});

                    auto result = kv_.commit(ops);
                    return {result.back().modifyIndex};

                } catch (TxnAborted&) {
                    throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) override
    {
        using namespace ppconsul::kv;

        return this->thread_pool_->async(
            [this, key = get_path_(key), value, version]() -> CASResult {
                if (!version)
                    return thread_pool_->then(
                        create(key.get_raw_key(), value),
                        [](std::future<CreateResult>&& result) -> CASResult {
                            try {
                                CreateResult unwrapped = util::call_get(std::move(result));
                                return {unwrapped.version, true};
                            } catch (EntryExists&) {
                                return {0, false};
                            }
                        }, true).get();

                try {
                    std::unique_lock lock(lock_);
                    auto result = kv_.commit({
                        txn_ops::Get{key},
                        txn_ops::CompareSet{key, static_cast<uint64_t>(version), value},
                    });
                    return {result.back().modifyIndex, true};

                } catch (TxnAborted& e) {
                    const auto opIndex = static_cast<size_t>(e.errors().front().opIndex);
                    if (opIndex == 0)
                        throw NoEntry{};
                    return {0, false};
                } liboffkv_catch
            });
    }

    std::future<GetResult> get(const std::string& key, bool watch = false) override
    {
        return thread_pool_->async(
            [this, key = get_path_(key), watch]() -> GetResult {
                std::unique_lock lock(lock_);
                try {
                    auto item = kv_.item(ppconsul::withHeaders, key);
                    if (!item.data().valid())
                        throw NoEntry{};

                    std::future<void> watch_future;
                    if (watch)
                        watch_future = get_watch_future_(key, item.headers().index());

                    return {item.headers().index(), item.data().value, watch_future.share()};
                } liboffkv_catch
            });
    }

    std::future<void> erase(const std::string& key, uint64_t version = 0) override
    {
        using namespace ppconsul::kv;

        return this->thread_pool_->async(
            [this, key = get_path_(key), version] {
                std::unique_lock lock(lock_);

                std::vector<TxnOperation> ops{
                    txn_ops::Get{key},
                };
                if (version) {
                    ops.push_back(txn_ops::CompareErase{key, static_cast<uint64_t>(version)});
                } else {
                    ops.push_back(txn_ops::Erase{key});
                    ops.push_back(txn_ops::EraseAll{static_cast<std::string>(key) + "/"});
                }

                try {
                    kv_.commit(ops);
                } catch (TxnAborted& e) {
                    const auto opIndex = static_cast<size_t>(e.errors().front().opIndex);
                    if (opIndex == 0)
                        throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<TransactionResult> commit(const Transaction& transaction) override
    {
        using namespace ppconsul::kv;

        enum class ResultKind {
            // This operation corresponds to a check user operation, produces 1 result.
            CHECK,

            // This operation corresponds to an op::op_type::CREATE user operation, produces 1 result.
            CREATE,

            // This operation corresponds to an op::op_type::SET user operation, produces 1 result.
            SET,

            // This operation corresponds to an op::op_type::ERASE user operation, produces no results.
            ERASE_NO_RESULT,

            // This is an auxiliary operation, produces 1 result.
            AUX,

            // This is an auxiliary operation, produces no results.
            AUX_NO_RESULT,
        };

        // We assume that each user operation maps to zero or more auxiliary (ResultKind::AUX or ResultKind::AUX_NO_RESULT)
        // operations, followed by exactly one non-auxiliary one.

        return thread_pool_->async([this, transaction] {
            std::unique_lock lock(lock_);

            std::vector<TxnOperation> ops;
            std::vector<ResultKind> result_kinds;

            const auto push_op = [&ops, &result_kinds](TxnOperation &&op, ResultKind rk) {
                ops.emplace_back(std::move(op));
                result_kinds.push_back(rk);
            };

            for (const auto& check : transaction.checks())
                if (check.version)
                    push_op(txn_ops::CheckIndex{get_path_(check.key), static_cast<uint64_t>(check.version)},
                            ResultKind::CHECK);
                else
                    push_op(txn_ops::Get{get_path_(check.key)}, ResultKind::CHECK);

            for (const auto& op_ptr : transaction.operations()) {
                switch (op_ptr->type) {
                    case op::op_type::CREATE: {
                        auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());

                        auto key = get_path_(create_op_ptr->key);
                        auto parent = key.get_parent();

                        if (!parent.empty())
                            push_op(txn_ops::Get{parent}, ResultKind::AUX);

                        push_op(txn_ops::CheckNotExists{key}, ResultKind::AUX_NO_RESULT);
                        push_op(txn_ops::Set{key, create_op_ptr->value}, ResultKind::CREATE);

                        break;
                    }
                    case op::op_type::SET: {
                        auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());

                        auto key = get_path_(set_op_ptr->key);
                        auto parent = key.get_parent();

                        if (!parent.empty())
                            push_op(txn_ops::Get{parent}, ResultKind::AUX);

                        push_op(txn_ops::Set{key, set_op_ptr->value}, ResultKind::SET);

                        break;
                    }
                    case op::op_type::ERASE: {
                        auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());

                        auto key = get_path_(op_ptr->key);

                        push_op(txn_ops::Get{key}, ResultKind::AUX);
                        push_op(txn_ops::Erase{key}, ResultKind::AUX_NO_RESULT);
                        push_op(txn_ops::EraseAll{static_cast<std::string>(key) + "/"},
                                ResultKind::ERASE_NO_RESULT);

                        break;
                    }
                }
            }

            try {
                const auto results = kv_.commit(ops);
                TransactionResult answer;

                size_t result_index = 0;
                for (ResultKind rk : result_kinds) {
                    switch (rk) {
                    case ResultKind::CREATE:
                        answer.push_back(CreateResult{results[result_index].modifyIndex});
                        ++result_index;
                        break;
                    case ResultKind::SET:
                        answer.push_back(SetResult{results[result_index].modifyIndex});
                        ++result_index;
                        break;
                    case ResultKind::CHECK:
                    case ResultKind::AUX:
                        ++result_index;
                        break;
                    case ResultKind::ERASE_NO_RESULT:
                    case ResultKind::AUX_NO_RESULT:
                        break;
                    }
                }

                return answer;

            } catch (TxnAborted& e) {
                const auto opIndex = static_cast<size_t>(e.errors().front().opIndex);

                size_t ncompleted = 0;

                for (size_t i = 0; i < opIndex; ++i) {
                    switch (result_kinds[i]) {
                    case ResultKind::CHECK:
                    case ResultKind::CREATE:
                    case ResultKind::SET:
                    case ResultKind::ERASE_NO_RESULT:
                        ++ncompleted;
                        break;
                    case ResultKind::AUX:
                    case ResultKind::AUX_NO_RESULT:
                        break;
                    }
                }

                throw TransactionFailed{ncompleted};
            }
        });
    }
};

} // namespace liboffkv
