#pragma once

#include <mutex>
#include "ppconsul/consul.h"
#include "ppconsul/kv.h"

#include "client_interface.hpp"



template <typename ThreadPool>
class ConsulClient : public Client<ThreadPool> {
private:
    using Consul = ppconsul::Consul;
    using Kv = ppconsul::kv::Kv;

    Consul client_;
    std::unique_ptr<Kv> kv_;
    mutable std::mutex lock_;

public:
    ConsulClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> time_machine)
        : Client<ThreadPool>(address, std::move(time_machine)),
          client_(Consul(address)), kv_(std::make_unique<Kv>(client_))
    {}

    ConsulClient() = delete;

    ConsulClient(const ConsulClient&) = delete;

    ConsulClient& operator=(const ConsulClient&) = delete;


    ~ConsulClient() = default;


    ConsulClient(ConsulClient&& another)
        : Client<ThreadPool>(std::move(another)),
          client_(std::move(another.client_)),
          kv_(std::make_unique<Kv>(client_))
    {}

    ConsulClient& operator=(ConsulClient&& another)
    {
        client_ = std::move(another.client_);
        kv_ = std::make_unique<Kv>(client_);
        this->thread_pool_ = std::move(another.thread_pool_);

        return *this;
    }


    // TODO: use lease
    // TODO in general: use transactions everywhere to check existance before update and so on atomically
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return this->thread_pool_->async(
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
        return this->thread_pool_->async(
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
        return this->thread_pool_->async(
                          [this, key, value]() -> SetResult {
                              std::unique_lock lock(lock_);
                              try {
                                  return {kv_->item(ppconsul::withHeaders, key).headers().index()};
                              } liboffkv_catch
                          });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version)
    {
        return this->thread_pool_->async(
                          [this, key, value, version]() -> CASResult {
                              std::unique_lock lock(lock_);
                              try {
                                  if (version < int64_t(0))
                                      return {set(key, value).get().version, true};
                                  auto success = kv_->compareSet(key, version, value);
                                  return {kv_->item(ppconsul::withHeaders, key).headers().index(), success};
                              } liboffkv_catch
                          });
    }

    std::future<GetResult> get(const std::string& key) const
    {
        return this->thread_pool_->async(
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

    std::future<void> erase(const std::string& key, int64_t version)
    {
        return this->thread_pool_->async(
                   [this, key, version] {
                       std::unique_lock lock(lock_);
                       try {
                           auto item = kv_->item(ppconsul::withHeaders, key);
                           if (!item.data().valid())
                               throw NoEntry{};

                           if (version < int64_t(0))
                               kv_->erase(key);
                           else
                               kv_->compareErase(key, version);
                       } liboffkv_catch
                   });
    }

    // TODO
    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        return this->thread_pool_->async([] { return TransactionResult(); });
    }
};
