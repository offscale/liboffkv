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
    static constexpr auto BLOCK_FOR_WHEN_WATCH = std::chrono::seconds(120);

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

    mutable std::mutex lock_;


    const Key get_path_(const std::string& key) const
    {
        Key res = key;
        res.set_prefix(prefix_);
        res.set_transformer([](const std::string& prefix, const std::vector<Key>& seq) {
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

    std::future<void> get_watch_future_(const std::string& key,
                                        uint64_t old_version,
                                        bool all_with_prefix = false) const
    {
        return std::async(std::launch::async, [this, key, old_version, all_with_prefix] {
            Consul client{address_};
            Kv kv{client, ppconsul::kw::consistency = CONSISTENCY};

            if (all_with_prefix)
                kv.keys(key, ppconsul::kv::kw::block_for = {BLOCK_FOR_WHEN_WATCH, old_version});
            else
                kv.item(key, ppconsul::kv::kw::block_for = {BLOCK_FOR_WHEN_WATCH, old_version});
        });
    };

public:
    ConsulClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> time_machine)
        : Client(address, prefix, std::move(time_machine)),
          client_(address),
          kv_(client_, ppconsul::kw::consistency = CONSISTENCY),
          sessions_(client_, ppconsul::kw::consistency = CONSISTENCY)
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
                    std::vector<TxnOperation> ops;

                    if (has_parent)
                        ops.push_back(txn_ops::Get{parent});

                    ops.push_back(txn_ops::CheckNotExists{key});

                    if (lease) {
                        auto id = sessions_.create("", std::chrono::seconds{15},
                                                   ppconsul::sessions::InvalidationBehavior::Delete, TTL);

                        ops.push_back(txn_ops::Lock{key, value, id});

                        thread_pool_->periodic(
                            [this, id = std::move(id)] {
                                try {
                                    sessions_.renew(id);
                                    return (TTL + std::chrono::seconds(1)) / 2;
                                } catch (... /* BadStatus& ??*/) {
                                    return std::chrono::seconds::zero();
                                }
                            }, (TTL + std::chrono::seconds(1)) / 2);
                    } else {
                        ops.push_back(txn_ops::Set{key, value});
                    }

                    return {kv_.commit(ops).back().modifyIndex};

                } catch (TxnAborted& e) {
                    const auto op_index = static_cast<size_t>(e.errors().front().opIndex);
                    if (has_parent && op_index == 0)
                        throw NoEntry{};
                    if (op_index == static_cast<size_t>(has_parent))
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
                const auto child_prefix = static_cast<std::string>(key) + "/";
                try {
                    auto res = kv_.commit({
                        txn_ops::GetAll{child_prefix},
                        txn_ops::Get{key},
                    });

                    std::future<void> watch_future;
                    if (watch)
                        watch_future = get_watch_future_(child_prefix, res.back().modifyIndex, true);

                    return {
                        util::map_vector(
                            util::filter_vector(
                                std::vector<KeyValue>(res.begin(), --res.end()),
                                [nprefix = child_prefix.size()](const auto& key_value) {
                                    return key_value.key.find('/', nprefix) == std::string::npos;
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
                    const auto op_index = static_cast<size_t>(e.errors().front().opIndex);
                    if (op_index == 0)
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
                    const auto op_index = static_cast<size_t>(e.errors().front().opIndex);
                    if (op_index == 0)
                        throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<TransactionResult> commit(const Transaction& transaction) override
    {
        using namespace ppconsul::kv;

        enum class ResultKind {
            CREATE,
            SET,
            AUX,
        };

        return thread_pool_->async([this, transaction] {
            std::unique_lock lock(lock_);

            std::vector<TxnOperation> ops;
            std::vector<size_t> boundaries;
            std::vector<ResultKind> result_kinds;

            for (const auto& check : transaction.checks()) {
                if (check.version)
                    ops.push_back(txn_ops::CheckIndex{
                        get_path_(check.key),
                        static_cast<uint64_t>(check.version)
                    });
                else
                    ops.push_back(txn_ops::Get{get_path_(check.key)});

                result_kinds.push_back(ResultKind::AUX);

                boundaries.push_back(ops.size() - 1);
            }

            for (const auto& op_ptr : transaction.operations()) {
                switch (op_ptr->type) {
                    case op::op_type::CREATE: {
                        auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());

                        auto key = get_path_(create_op_ptr->key);
                        auto parent = key.get_parent();

                        if (!parent.empty()) {
                            ops.push_back(txn_ops::Get{parent});
                            result_kinds.push_back(ResultKind::AUX);
                        }

                        ops.push_back(txn_ops::CheckNotExists{key});
                        // txn_ops::CheckNotExists does not produce any results

                        ops.push_back(txn_ops::Set{key, create_op_ptr->value});
                        result_kinds.push_back(ResultKind::CREATE);

                        break;
                    }
                    case op::op_type::SET: {
                        auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());

                        auto key = get_path_(set_op_ptr->key);

                        ops.push_back(txn_ops::Get{key});
                        result_kinds.push_back(ResultKind::AUX);

                        ops.push_back(txn_ops::Set{key, set_op_ptr->value});
                        result_kinds.push_back(ResultKind::SET);

                        break;
                    }
                    case op::op_type::ERASE: {
                        auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());

                        auto key = get_path_(op_ptr->key);

                        ops.push_back(txn_ops::Get{key});
                        result_kinds.push_back(ResultKind::AUX);

                        ops.push_back(txn_ops::Erase{key});
                        // txn_ops::Erase does not produce any results
                        ops.push_back(txn_ops::EraseAll{static_cast<std::string>(key) + "/"});
                        // txn_ops::EraseAll does not produce any results

                        break;
                    }
                }

                boundaries.push_back(ops.size() - 1);
            }

            try {
                const auto results = kv_.commit(ops);
                TransactionResult answer;
                for (size_t i = 0; i < results.size(); ++i) {
                    switch (result_kinds[i]) {
                        case ResultKind::CREATE:
                            answer.push_back(CreateResult{results[i].modifyIndex});
                            break;
                        case ResultKind::SET:
                            answer.push_back(SetResult{results[i].modifyIndex});
                            break;
                        case ResultKind::AUX:
                            break;
                    }
                }
                return answer;

            } catch (TxnAborted& e) {
                const auto op_index = static_cast<size_t>(e.errors().front().opIndex);
                throw TransactionFailed{util::user_op_index(boundaries, op_index)};
            }
        });
    }
};

} // namespace liboffkv
