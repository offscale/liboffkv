#pragma once

#include "ppconsul/consul.h"
#include "ppconsul/kv.h"


#include "client_interface.hpp"



class ConsulClient : public Client {
private:
    using Consul = ppconsul::Consul;
    using Kv = ppconsul::kv::Kv;

    Consul client_;
    std::unique_ptr<Kv> kv_;

public:
    ConsulClient(const std::string& address)
        : client_(Consul(address)),
          kv_(std::make_unique<Kv>(client_))
    {}

    ConsulClient() = delete;
    ConsulClient(const ConsulClient&) = delete;
    ConsulClient& operator=(const ConsulClient&) = delete;

    
    ~ConsulClient() {}


    ConsulClient(ConsulClient&& another)
        : client_(std::move(another.client_)),
          kv_(std::make_unique<Kv>(client_))
    {}
    
    ConsulClient& operator=(ConsulClient&& another)
    {
        client_ = std::move(another.client_);
        kv_ = std::make_unique<Kv>(client_);

        return *this;
    }


    void create(const std::string& key, const std::string& value, const std::string& lease = {})
    {
        liboffkv_try([&] {
            kv_->set(key, value);
        });
    }

    bool exists(const std::string& key) const
    {
        return liboffkv_try([&] {
            return kv_->item(key).valid();
        });
    }

    void set(const std::string& key, const std::string& value)
    {
        return liboffkv_try([&] {
            kv_->set(key, value);
        });
    }

    void cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        liboffkv_try([&] {
            if (version < 0)
                set(key, value);
            else
                kv_->compareSet(key, version, value);
        });
    }

    std::string get(const std::string& key) const
    {
        return liboffkv_try([&] {
            auto result = kv_->get(key, {});
            if (result == std::string{})
                throw NoEntry{};
            return result;
        });
    }

    void erase(const std::string& key, int64_t version = 0)
    {
        return liboffkv_try([&] {
        if (version < 0)
            kv_->erase(key);
        else
            kv_->compareErase(key, version);
        });
    }
};
