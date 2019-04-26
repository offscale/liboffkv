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


    void set(const std::string& key, const std::string& value)
    {
        kv_->set(key, value);
    }

    std::string get(const std::string& key, const std::string& default_value) const
    {
        return kv_->get(key, default_value);
    }
};
