#pragma once

#include <zk/client.hpp>
#include <zk/error.hpp>
#include <zk/multi.hpp>
#include <zk/types.hpp>


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
        return KV_STORAGE_ZNODE + key;
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


    void create(const std::string& key, const std::string& value, const std::string& lease = {})
    {
        liboffkv_try([&] {
            client_.create(get_path(key), from_string(value));
        });
    }

    bool exists(const std::string& key) const
    {
        return liboffkv_try([&] {
            return !!client_.exists(get_path(key)).get();
        });
    }

    void set(const std::string& key, const std::string& value)
    {
        liboffkv_try([&] {
            auto path = get_path(key);

            try {
                client_.create(path, from_string(value));
            } catch (zk::entry_exists &) {
                client_.set(path, from_string(value));
            }
        });
    }

    void cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        liboffkv_try([&] {
            if (version < 0)
                set(key, value);
            else if (!version) {
                try {
                    create(key, value);
                } catch (zk::entry_exists&) {
                    client_.set(get_path(key), from_string(value), zk::version(0));
                }
            }
            else
                client_.set(get_path(key), from_string(value), zk::version(version));
        });
    }

    std::string get(const std::string& key) const
    {
        return liboffkv_try([&] {
            return to_string(client_.get(get_path(key)).get().data());
        });
    }

    void erase(const std::string& key, int64_t version = 0)
    {
        liboffkv_try([&] {
            auto path = get_path(key);

            if (version < 0)
                client_.erase(path);
            else
                client_.erase(path, zk::version(version));
        });
    }
};

const std::string ZKClient::KV_STORAGE_ZNODE = "/__kv";
