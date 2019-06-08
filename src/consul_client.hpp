#pragma once

#include <mutex>
#include "ppconsul/consul.h"
#include "ppconsul/kv.h"

#include "client_interface.hpp"
#include "key.hpp"


namespace liboffkv {

class ConsulClient : public Client {
private:
    static constexpr auto BLOCK_FOR_WHEN_WATCH = std::chrono::seconds(20);

    static constexpr auto TTL = std::chrono::seconds(10);

    using Consul = ppconsul::Consul;
    using Kv = ppconsul::kv::Kv;
    using TxnRequest = ppconsul::kv::TxnRequest;
    using TxnError = ppconsul::kv::TxnError;

    using Key = key::Key;

    Consul client_;
    std::unique_ptr<Kv> kv_;

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
            kv_->item(key, ppconsul::kv::kw::block_for = {BLOCK_FOR_WHEN_WATCH, old_version});
        });
    };

public:
    ConsulClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> time_machine)
        : Client(std::move(time_machine)),
          client_(Consul(address)),
          kv_(std::make_unique<Kv>(client_, ppconsul::kw::consistency = ppconsul::Consistency::Consistent)),
          prefix_(prefix)
    {}

    ConsulClient() = delete;

    ConsulClient(const ConsulClient&) = delete;

    ConsulClient& operator=(const ConsulClient&) = delete;


    ~ConsulClient() = default;


    std::future<CreateResult> create(const std::string& key, const std::string& value, bool lease = false) override
    {
        return this->thread_pool_->async(
            [this, key = get_path_(key), value, lease]() -> CreateResult {
                std::unique_lock lock(lock_);

                auto parent = key.get_parent();
                bool has_parent = (parent.size() > 0);

                try {
                    if (lease) {
                        auto id = kv_->createSession(key, 15, ppconsul::kv::SessionDropBehavior::DELETE, TTL.count());
                        thread_pool_->periodic(
                            [this, id = std::move(id)] {
                                try {
                                    kv_->renewSession(id);
                                    return (TTL + std::chrono::seconds(1)) / 2;
                                } catch (... /* BadStatus& ??*/) {
                                    return std::chrono::seconds::zero();
                                }
                            }, (TTL + std::chrono::seconds(1)) / 2);
                    }

                    std::vector<TxnRequest> requests;

                    if (has_parent)
                        requests.push_back(TxnRequest::get(parent));

                    requests.push_back(TxnRequest::checkNotExists(key));
                    requests.push_back(TxnRequest::set(key, value));

                    return {kv_->commit(requests).back().modifyIndex};

                } catch (TxnError& e) {
                    if (has_parent && (e.index() == 0))
                        throw NoEntry{};
                    if (e.index() == static_cast<uint64_t>(has_parent))
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
                    auto item = kv_->item(ppconsul::withHeaders, key);

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
        return this->thread_pool_->async(
            [this, key = get_path_(key), watch]() -> ChildrenResult {
                std::unique_lock lock(lock_);
                try {

                    auto res = kv_->commit({
                                               TxnRequest::getAll(key),
                                               TxnRequest::get(key)
                                           });

                    std::future<void> watch_future;
                    if (watch)
                        watch_future = get_watch_future_(key, res.back().modifyIndex);

                    return {
                        util::map_vector(
                            util::filter_vector(
                                std::vector<ppconsul::kv::KeyValue>(res.begin(), --res.end()),
                                [key = static_cast<std::string>(key)](const auto& key_value) {
                                    return key_value.key != key &&
                                           key_value.key.substr(key.size() + 1).find('/') == std::string::npos;
                                }),
                            [this](const auto& key_value) {
                                return detach_prefix_(key_value.key);
                            }),
                        watch_future.share()
                    };
                } catch (TxnError& e) {
                    throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value) override
    {
        return thread_pool_->async(
            [this, key = get_path_(key), value]() -> SetResult {
                std::unique_lock lock(lock_);
                try {
                    auto parent = key.get_parent();

                    std::vector<TxnRequest> requests;

                    if (!parent.empty())
                        requests.push_back(TxnRequest::get(parent));

                    requests.push_back(TxnRequest::set(key, value));

                    auto result = kv_->commit(requests);
                    return {result.back().modifyIndex};

                } catch (TxnError&) {
                    throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) override
    {
        return this->thread_pool_->async(
            [this, key = get_path_(key), value, version]() -> CASResult {
                if (!version)
                    return thread_pool_->then(
                        create(key.get_raw_key(), value),
                        [](std::future<CreateResult>&& result) -> CASResult {
                            CreateResult unwrapped;

                            try {
                                unwrapped = util::call_get(std::move(result));
                                return {unwrapped.version, true};
                            } catch (EntryExists&) {
                                return {0, false};
                            }
                        }, true).get();

                try {
                    std::unique_lock lock(lock_);
                    auto result = kv_->commit({
                                                  TxnRequest::get(key),
                                                  TxnRequest::compareSet(key, static_cast<uint64_t>(version),
                                                                         value)
                                              });

                    return {result.back().modifyIndex, true};
                } catch (TxnError& e) {
                    if (e.index() == 0)
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
                    auto item = kv_->item(ppconsul::withHeaders, key);
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
        return this->thread_pool_->async(
            [this, key = get_path_(key), version] {
                std::unique_lock lock(lock_);
                try {
                    std::vector<TxnRequest> requests;

                    requests.push_back(TxnRequest::get(key));
                    requests.push_back(version ? TxnRequest::compareErase(key, static_cast<uint64_t>(version))
                                               : TxnRequest::eraseAll(key));

                    kv_->commit(requests);

                } catch (TxnError& e) {
                    if (e.index() == 0)
                        throw NoEntry{};
                } liboffkv_catch
            });
    }

    std::future<TransactionResult> commit(const Transaction& transaction) override
    {
        return thread_pool_->async([this, transaction] {
            std::vector<TxnRequest> requests;

            for (const auto& check : transaction.checks())
                requests.push_back(check.version ? TxnRequest::checkIndex(get_path_(check.key),
                                                                          static_cast<uint64_t>(check.version))
                                                 : TxnRequest::get(get_path_(check.key)));

            std::vector<int> set_indices, create_indices, support_indices;
            int _j = transaction.checks().size();

            for (const auto& op_ptr : transaction.operations()) {
                switch (op_ptr->type) {
                    case op::op_type::CREATE: {
                        auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());

                        auto key = get_path_(create_op_ptr->key);
                        auto parent = key.get_parent();

                        if (parent.size()) {
                            requests.push_back(TxnRequest::get(parent));
                            support_indices.push_back(_j++);
                        }

                        requests.push_back(TxnRequest::checkNotExists(key));
                        support_indices.push_back(_j++);

                        requests.push_back(TxnRequest::set(
                            key,
                            create_op_ptr->value
                        ));

                        create_indices.push_back(_j++);

                        break;
                    }
                    case op::op_type::SET: {
                        auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());

                        auto key = get_path_(set_op_ptr->key);
                        auto parent = key.get_parent();

                        if (parent.size()) {
                            requests.push_back(TxnRequest::get(parent));
                            support_indices.push_back(_j++);
                        }

                        requests.push_back(TxnRequest::set(
                            key,
                            set_op_ptr->value
                        ));

                        set_indices.push_back(_j++);
                        break;
                    }
                    case op::op_type::ERASE: {
                        auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());

                        auto key = get_path_(op_ptr->key);

                        requests.push_back(TxnRequest::get(key));
                        support_indices.push_back(_j++);

                        requests.push_back(TxnRequest::eraseAll(key));

                        ++_j;
                        break;
                    }
                    default:
                        __builtin_unreachable();
                }
            };

            try {
                auto commit_result = kv_->commit(requests);
                TransactionResult result;

                int i = 0, j = 0;
                while (i < create_indices.size() && j < set_indices.size())
                    if (create_indices[i] < set_indices[j]) {
                        result.push_back(CreateResult{commit_result[create_indices[i]].modifyIndex});
                        ++i;
                    } else {
                        result.push_back(SetResult{commit_result[set_indices[j]].modifyIndex});
                        ++j;
                    }

                while (i < create_indices.size())
                    result.push_back(CreateResult{commit_result[create_indices[i++]].modifyIndex});

                while (j < set_indices.size())
                    result.push_back(SetResult{commit_result[set_indices[j++]].modifyIndex});

                return result;
            } catch (TxnError& e) {
                auto it = std::upper_bound(support_indices.begin(), support_indices.end(), e.index());

                throw TransactionFailed{static_cast<size_t>(
                    e.index() - std::distance(support_indices.begin(), it) + (e.index() == *(it - 1)))};
            }
        });
    }
};

} // namespace liboffkv