#pragma once

#include <zk/client.hpp>
#include <zk/multi.hpp>


#include "client_interface.hpp"



class ZKClient : public Client {
private:
    using buffer = zk::buffer;
    const static std::string KV_STORAGE_ZNODE;

    static
    buffer from_string(const std::string& str)
    {
        return {str.begin(), str.end()};
    }

    static
    std::string to_string(const buffer& buf)
    {
        return {buf.begin(), buf.end()};
    }

    static
    std::string get_path(const std::string& key)
    {
        return KV_STORAGE_ZNODE + "/" + key;
    }

    zk::client client_;


public:
    ZKClient(const std::string& address)
        : client_(zk::client::connect(address).get())
    {
        client_.create(KV_STORAGE_ZNODE, buffer());
    }

    ZKClient() = delete;
    ZKClient(const ZKClient&) = delete;
    ZKClient& operator=(const ZKClient&) = delete;

    
    ~ZKClient() {}


    ZKClient(ZKClient&& another)
        : client_(std::move(another.client_))
    {}
    
    ZKClient& operator=(ZKClient&& another)
    {
        client_ = std::move(another.client_);

        return *this;
    }


    void set(const std::string& key, const std::string& value)
    {
        auto path = get_path(key);

        if (!client_.exists(path).get())
            client_.create(path, from_string(value));
        else
            client_.set(path, from_string(value));
    }

    std::string get(const std::string& key, const std::string& default_value) const
    {
        auto path = get_path(key);

        if (!client_.exists(path).get())
            return default_value;
        return to_string(client_.get(path).get().data());
    }
};

const std::string ZKClient::KV_STORAGE_ZNODE = "/__kv";
