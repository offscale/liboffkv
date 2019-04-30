#pragma once

#include <zk/client.hpp>
#include <zk/error.hpp>
#include <zk/multi.hpp>
#include <zk/types.hpp>


#include "client_interface.hpp"
#include "time_machine.hpp"
#include "util.hpp"



template <typename TimeMachine = time_machine::TimeMachine<>>
class ZKClient : public Client<TimeMachine> {
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
    ZKClient(const std::string& address, std::shared_ptr<TimeMachine> time_machine = nullptr)
        : Client<TimeMachine>(address, time_machine), client_(zk::client::connect(address).get())
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
        this->time_machine_ = std::move(another.time_machine_);

        return *this;
    }


    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return liboffkv_try([this, &key, &value, lease] {
            return this->time_machine_->then(
                client_.create(get_path(key), from_string(value),
                               lease ? zk::create_mode::normal
                                     : zk::create_mode::ephemeral), ignore);
        });
    }

    std::future<ExistsResult> exists(const std::string& key) const
    {
        return liboffkv_try([this, &key] {
            return this->time_machine_->then(client_.exists(get_path(key)), [](auto&& result) -> ExistsResult {
                auto unwrapped = result.get();
                auto stat = unwrapped.stat();

                return {stat.has_value() ? static_cast<int64_t>(stat.value().data_version.value) : -1, !!unwrapped};
            });
        });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        return liboffkv_try([this, &key, &value] {
            auto path = get_path(key);

            try {
                return this->time_machine_->then(client_.create(path, from_string(value)), [](auto&&) -> SetResult {
                    return {0};
                });
            } catch (zk::entry_exists&) {
                return this->time_machine_->then(client_.set(path, from_string(value)), [](auto&& result) -> SetResult {
                    return {result.get().stat().data_version.value};
                });
            }
        });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        return liboffkv_try([this, &key, &value, version] {
            if (version < 0)
                return this->time_machine_->then(set(key, value), [](auto&& set_result) -> CASResult {
                    return {set_result.get().version, true};
                });
            if (!version) {
                try {
                    return this->time_machine_->then(create(key, value), [](auto&&) -> CASResult {
                            return {0, true};
                    });
                } catch (zk::entry_exists&) {

                }
            }
            return this->time_machine_->then(
                client_.set(get_path(key), from_string(value), zk::version(version)),
                [version](auto&& result) -> CASResult {
                    auto new_version = static_cast<int64_t>(result.get().stat().data_version.value);
                    return {new_version, version == new_version};
                });
        });
    }

    std::future<GetResult> get(const std::string& key) const
    {
        return liboffkv_try([this, &key] {
            return this->time_machine_->then(client_.get(get_path(key)), [](auto&& result) -> GetResult {
                return {result.get().stat().data_version.value, to_string(result.get().data())};
            });
        });
    }

    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        return liboffkv_try([this, &key, version] {
            auto path = get_path(key);

            if (version < 0)
                return this->time_machine_->then(client_.erase(path), ignore);
            return this->time_machine_->then(client_.erase(path, zk::version(version)), ignore);
        });
    }
};

template <typename TimeMachine>
const std::string ZKClient<TimeMachine>::KV_STORAGE_ZNODE = "/__kv";
