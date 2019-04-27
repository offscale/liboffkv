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
        kv_->set(key, value);
    }

    bool exists(const std::string& key) const
    {
        return kv_->item(key).valid();
    }

    void set(const std::string& key, const std::string& value)
    {
        kv_->set(key, value);
    }

    void cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        if (version < 0)
            set(key, value);
        else if (!version && !exists(key))
            create(key, value);
        else
            kv_->compareSet(key, version, value);
    }

    std::string get(const std::string& key) const
    {
        if (!exists(key))
            throw 1;
        return kv_->get(key, {});
    }

    void erase(const std::string& key, int64_t version = 0)
    {
        if (version < 0)
            kv_->erase(key);
        else
            kv_->compareErase(key, version);
    }
};
