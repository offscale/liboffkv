#pragma once

#include "ppconsul/consul.h"
#include "ppconsul/kv.h"


#include "client_interface.hpp"



template <typename TimeMachine>
class ConsulClient : public Client<TimeMachine> {
private:
    using Consul = ppconsul::Consul;
    using Kv = ppconsul::kv::Kv;

    Consul client_;
    std::unique_ptr<Kv> kv_;

public:
    ConsulClient(const std::string& address, std::shared_ptr<TimeMachine> time_machine)
        : Client<TimeMachine>(address, time_machine),
          client_(Consul(address)), kv_(std::make_unique<Kv>(client_))
    {}

    ConsulClient() = delete;
    ConsulClient(const ConsulClient&) = delete;
    ConsulClient& operator=(const ConsulClient&) = delete;

    
    ~ConsulClient() {}


    ConsulClient(ConsulClient&& another)
        : Client<TimeMachine>(std::move(another)),
          client_(std::move(another.client_)),
          kv_(std::make_unique<Kv>(client_))
    {}

    ConsulClient& operator=(ConsulClient&& another)
    {
        client_ = std::move(another.client_);
        kv_ = std::make_unique<Kv>(client_);
        this->time_machine_ = std::move(another.time_machine_);

        return *this;
    }



    // TODO: use lease, consider blocking queries
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return std::async(std::launch::async,
            [this, key, value] {
                liboffkv_try([this, key = std::move(key), value = std::move(value)] {
                    if (kv_->count(key))
                        throw EntryExists{};
                    kv_->set(key, value);
                });
            });
    }

    std::future<ExistsResult> exists(const std::string& key) const
    {
        return std::async(std::launch::async,
            [this, key] {
                return liboffkv_try([this, key = std::move(key)]() -> ExistsResult {
                    auto item = kv_->item(ppconsul::withHeaders, key);
                    return {item.headers().index(), item.data().valid()};
                });
            });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        return std::async(std::launch::async,
            [this, key, value] {
                return liboffkv_try([this, key = std::move(key), value = std::move(value)]() -> SetResult {
                    return {kv_->item(ppconsul::withHeaders, key).headers().index()};
                });
            });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        return std::async(std::launch::async,
            [this, key, value, version] {
                return liboffkv_try([this, key = std::move(key), value = std::move(value), version]() -> CASResult {
                    if (version < 0)
                        return {set(key, value).get().version, true};
                    auto success = kv_->compareSet(key, version, value);
                    return {kv_->item(ppconsul::withHeaders, key).headers().index(), success};
                });
            });
    }

    std::future<GetResult> get(const std::string& key) const
    {
        return std::async(std::launch::async,
            [this, key] {
                return liboffkv_try([this, key = std::move(key)]() -> GetResult {
                    auto item = kv_->item(ppconsul::withHeaders, std::move(key));
                    if (!item.data().valid())
                        throw NoEntry{};
                    return {item.headers().index(), item.data().value};
                });
            });
    }

    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        std::async(std::launch::async,
            [this, key, version] {
                liboffkv_try([this, key = std::move(key), version] {
                    if (version < 0)
                        kv_->erase(key);
                    else
                        kv_->compareErase(key, version);
                });
            });
    }

    // TODO
    std::future<TransactionResult> commit(const Transaction& transaction) {
        return std::async(std::launch::async, [] {return TransactionResult();});
    }
};
