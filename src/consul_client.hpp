#pragma once

#include <mutex>
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
    mutable std::mutex lock_;

public:
    ConsulClient(const std::string& address, std::shared_ptr<TimeMachine> time_machine)
        : Client<TimeMachine>(address, std::move(time_machine)),
          client_(Consul(address)), kv_(std::make_unique<Kv>(client_))
    {}

    ConsulClient() = delete;

    ConsulClient(const ConsulClient&) = delete;

    ConsulClient& operator=(const ConsulClient&) = delete;


    ~ConsulClient() = default;


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
                          [this, key, value, lease] {
                              std::unique_lock lock(lock_);
                              try {
                                  if (kv_->count(key))
                                      throw EntryExists{};
                                  kv_->set(key, value);
                              } liboffkv_catch
                          });
    }

    std::future<ExistsResult> exists(const std::string& key) const
    {
        return std::async(std::launch::async,
                          [this, key]() -> ExistsResult {
                              std::unique_lock lock(lock_);
                              try {
                                  auto item = kv_->item(ppconsul::withHeaders, key);
                                  return {item.headers().index(), item.data().valid()};
                              } liboffkv_catch
                          });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        return std::async(std::launch::async,
                          [this, key, value]() -> SetResult {
                              std::unique_lock lock(lock_);
                              try {
                                  return {kv_->item(ppconsul::withHeaders, key).headers().index()};
                              } liboffkv_catch
                          });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        return std::async(std::launch::async,
                          [this, key, value, version]() -> CASResult {
                              std::unique_lock lock(lock_);
                              try {
                                  if (version < 0)
                                      return {set(key, value).get().version, true};
                                  auto success = kv_->compareSet(key, version, value);
                                  return {kv_->item(ppconsul::withHeaders, key).headers().index(), success};
                              } liboffkv_catch
                          });
    }

    std::future<GetResult> get(const std::string& key) const
    {
        return std::async(std::launch::async,
                          [this, key]() -> GetResult {
                              std::unique_lock lock(lock_);
                              try {
                                  auto item = kv_->item(ppconsul::withHeaders, std::move(key));
                                  if (!item.data().valid())
                                      throw NoEntry{};
                                  return {item.headers().index(), item.data().value};
                              } liboffkv_catch
                          });
    }

    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        std::async(std::launch::async,
                   [this, key, version] {
                       std::unique_lock lock(lock_);
                       try {
                           if (version < 0)
                               kv_->erase(key);
                           else
                               kv_->compareErase(key, version);
                       } liboffkv_catch
                   });
    }

    // TODO
    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        return std::async(std::launch::async, [] { return TransactionResult(); });
    }
};
