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
    ZKClient(const std::string& address, std::shared_ptr<TimeMachine> time_machine)
        : Client<TimeMachine>(address, std::move(time_machine)), client_(zk::client::connect(address).get())
    {
        client_.create(KV_STORAGE_ZNODE, buffer());
    }

    ZKClient() = delete;
    ZKClient(const ZKClient&) = delete;
    ZKClient& operator=(const ZKClient&) = delete;

    
    ~ZKClient() {}


    ZKClient(ZKClient&& another)
        : Client<TimeMachine>(another), client_(std::move(another.client_))
    {}
    
    ZKClient& operator=(ZKClient&& another)
    {
        client_ = std::move(another.client_);
        this->time_machine_ = std::move(another.time_machine_);

        return *this;
    }


    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return this->time_machine_->then(
            client_.create(get_path(key), from_string(value),
                           lease ? zk::create_mode::normal
                                 : zk::create_mode::ephemeral), call_get_then_ignore);
    }

    std::future<ExistsResult> exists(const std::string& key) const
    {
        return this->time_machine_->then(client_.exists(get_path(key)), [](auto&& result) -> ExistsResult {
            auto unwrapped = call_get(result);
            auto stat = unwrapped.stat();

            return {stat.has_value() ? static_cast<int64_t>(stat.value().data_version.value) : -1, !!unwrapped};
        });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        auto path = get_path(key);

        return this->time_machine_->then(
            this->time_machine_->then(client_.create(path, from_string(value)), [path, value](auto&& res) -> SetResult {
                call_get_then_ignore(res);
                return {0};
            }),
            [this, path, value](auto&& set_res_future) -> SetResult {
                try {
                    return call_get(set_res_future);
                } catch (EntryExists&) {
                    return call_get(this->time_machine_->then(client_.set(path, from_string(value)),
                                                              [](auto&& result) -> SetResult {
                                                                  return {call_get(result).stat().data_version.value};
                                                              }));
                }
            });
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        if (version < 0)
            return this->time_machine_->then(set(key, value), [](auto&& set_result) -> CASResult {
                return {call_get(set_result).version};
            });

        auto path = get_path(key);

        if (!version) {
            return this->time_machine_->then(create(key, value), [this, path, value](auto&& res) -> CASResult {
                try {
                    call_get_then_ignore(res);
                    return {0};
                } catch (EntryExists&) {
                    return call_get(this->time_machine_->then(
                        client_.set(path, from_string(value), zk::version(0)),
                        [](auto&& result) -> CASResult {
                            return {static_cast<int64_t>(call_get(result).stat().data_version.value)};
                        }));
                }
            });
        }
        return this->time_machine_->then(
            client_.set(get_path(key), from_string(value), zk::version(version)),
            [version](auto&& result) -> CASResult {
                return {static_cast<int64_t>(call_get(result).stat().data_version.value)};
            });
    }

    std::future<GetResult> get(const std::string& key) const
    {
        return this->time_machine_->then(client_.get(get_path(key)), [](auto&& result) -> GetResult {
            auto res = call_get(result);
            return {res.stat().data_version.value, to_string(res.data())};
        });
    }

    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        auto path = get_path(key);

        if (version < 0)
            return this->time_machine_->then(client_.erase(path), call_get_then_ignore);
        return this->time_machine_->then(client_.erase(path, zk::version(version)), call_get_then_ignore);
    }

    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        zk::multi_op trn;

        for (const auto& op_ptr : transaction) {
            switch (op_ptr->type) {
                case op::op_type::CREATE: {
                    auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());
                    trn.push_back(zk::op::create(create_op_ptr->key, from_string(create_op_ptr->value)));
                    break;
                }
                case op::op_type::CHECK: {
                    auto check_op_ptr = dynamic_cast<op::Check*>(op_ptr.get());
                    trn.push_back(zk::op::check(check_op_ptr->key, zk::version(check_op_ptr->version)));
                    break;
                }
                case op::op_type::SET: {
                    auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());
                    trn.push_back(zk::op::set(set_op_ptr->key, from_string(set_op_ptr->value)));
                    break;
                }
                case op::op_type::ERASE: {
                    auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());
                    trn.push_back(zk::op::erase(erase_op_ptr->key, zk::version(erase_op_ptr->version)));
                    break;
                }
                default: __builtin_unreachable();
            }
        }

        return this->time_machine_->then(client_.commit(trn), [](auto&& multi_res) {
            TransactionResult result;

            auto multi_res_unwrapped = call_get(multi_res);

            for (const auto& res : multi_res_unwrapped) {
                switch (res.type()) {
                    case zk::op_type::create:
                        result.emplace_back(op::op_type::CREATE, std::make_shared<CreateResult>());
                        break;
                    case zk::op_type::set:
                        result.emplace_back(op::op_type::SET,
                                            std::make_shared<SetResult>(SetResult{
                                                res.as_set().stat().data_version.value
                                            }));
                        break;
                    case zk::op_type::check:
                    case zk::op_type::erase:
                    default: __builtin_unreachable();
                }
            }

            return result;
        });
    }
};

template <typename TimeMachine>
const std::string ZKClient<TimeMachine>::KV_STORAGE_ZNODE = "/__KV__";
