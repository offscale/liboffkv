#pragma once

#include <mutex>
#include "ppconsul/consul.h"
#include "ppconsul/kv.h"

#include "client_interface.hpp"
#include "key.hpp"



class ConsulClient : public Client {
private:
    static constexpr auto BLOCK_FOR_WHEN_WATCH = std::chrono::minutes(1);

    static constexpr auto TTL = std::chrono::seconds(10);

    using Consul = ppconsul::Consul;
    using Kv = ppconsul::kv::Kv;
    using TxnRequest = ppconsul::kv::TxnRequest;
    using TxnError = ppconsul::kv::TxnError;

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
        return full_path.substr(prefix_.size() + 1);
    }

    std::future<void> get_watch_future_(const std::string& key, uint64_t old_version) const
    {
        return std::async([this, key, old_version] {
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

                    std::vector <TxnRequest> requests;

                    auto parent = key.get_parent();

                    if (parent.size()) {
                        requests.push_back(TxnRequest::get(parent));
                    }

                    requests.push_back(TxnRequest::checkNotExists(key));
                    requests.push_back(TxnRequest::set(key, value));

                    return {kv_->commit(requests).back().modifyIndex};

                } catch (TxnError&) {
                    throw EntryExists{};
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
                        map_vector(std::vector<ppconsul::kv::KeyValue>(res.begin(), --res.end()),
                                   [this](const auto& key_value) {
                                       return detach_prefix_(key_value.key);
                                   }),
                        watch_future.share()
                    };
                } catch (TxnError&) {
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
                std::unique_lock lock(lock_);
                try {
                    if (!version)
                        return {set(key, value).get().version, true};

                    auto result = kv_->commit({
                                                  TxnRequest::get(key),
                                                  TxnRequest::compareSet(key, static_cast<uint64_t>(version - 1),
                                                                         value)
                                              });

                    return {result.back().modifyIndex,
                        /* is it correct? */ result.front().modifyIndex != result.back().modifyIndex};
                } catch (TxnError&) {
                    throw NoEntry{};
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
                    requests.push_back(version ? TxnRequest::compareErase(key, static_cast<uint64_t>(version - 1))
                                               : TxnRequest::eraseAll(key));

                    kv_->commit(requests);

                } catch (TxnError&) {
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
                                                                          static_cast<uint64_t>(check.version - 1))
                                                 : TxnRequest::get(get_path_(check.key)));

            std::vector<int> set_indices, create_indices;
            int _j = transaction.checks().size();

            for (const auto& op_ptr : transaction.operations()) {
                switch (op_ptr->type) {
                    // fix it
                    case op::op_type::CREATE: {
                        auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());
                        requests.push_back(TxnRequest::set(
                            get_path_(create_op_ptr->key),
                            create_op_ptr->value
                        ));

                        create_indices.push_back(_j++);
                        break;
                    }
                    case op::op_type::SET: {
                        auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());
                        requests.push_back(TxnRequest::set(
                            get_path_(set_op_ptr->key),
                            set_op_ptr->value
                        ));

                        set_indices.push_back(_j++);
                        break;
                    }
                    case op::op_type::ERASE: {
                        auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());
                        requests.push_back(TxnRequest::eraseAll(get_path_(erase_op_ptr->key)));

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
                        result.push_back(CreateResult{1});
                        ++i;
                    } else {
                        result.push_back(SetResult{commit_result[set_indices[j]].modifyIndex});
                        ++j;
                    }

                while (i++ < create_indices.size())
                    result.push_back(CreateResult{commit_result[set_indices[j++]].modifyIndex});

                while (j < set_indices.size())
                    result.push_back(SetResult{commit_result[set_indices[j++]].modifyIndex});

                return result;
            } catch (TxnError& e) {
                throw TransactionFailed{static_cast<size_t>(e.index())};
            }
        });
    }
};
