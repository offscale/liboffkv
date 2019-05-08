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
        : Client<TimeMachine>(address, std::move(time_machine)),
          client_(zk::client::connect(address).get())
    {
        client_.create(KV_STORAGE_ZNODE, buffer());
    }

    ZKClient() = delete;

    ZKClient(const ZKClient&) = delete;

    ZKClient& operator=(const ZKClient&) = delete;


    ~ZKClient() = default;


    ZKClient(ZKClient&& another)
        : Client<TimeMachine>(std::move(another)),
          client_(std::move(another.client_))
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
            client_.create(
                get_path(key),
                from_string(value),
                !lease ? zk::create_mode::normal : zk::create_mode::ephemeral
            ),
            call_get_ignore<zk::create_result>
        );
    }

    std::future<ExistsResult> exists(const std::string& key) const
    {
        return this->time_machine_->then(client_.exists(get_path(key)),
                                         [](std::future<zk::exists_result>&& result) -> ExistsResult {
                                             zk::exists_result unwrapped = call_get(std::move(result));
                                             auto stat = unwrapped.stat();

                                             return {stat.has_value()
                                                     ? static_cast<int64_t>(stat.value().data_version.value) : -1,
                                                     !!unwrapped};
                                         });
    }

    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        auto path = get_path(key);

        return this->time_machine_->then(
            this->time_machine_->then(
                client_.create(path, from_string(value)),
                [](std::future<zk::create_result>&& res) -> SetResult {
                    call_get(std::move(res));
                    return {0};
                }
            ),
            [this, path, value](std::future<SetResult>&& set_res_future) -> SetResult {
                try {
                    return call_get(std::move(set_res_future));
                } catch (EntryExists&) {
                    return call_get(this->time_machine_->then(
                        client_.set(path, from_string(value)),
                        [path](std::future<zk::set_result>&& result) -> SetResult {
                            return {call_get(std::move(result)).stat().data_version.value};
                        },
                        true
                    ));
                }
            }
        );
    }

    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version)
    {
        if (version < int64_t(0))
            return this->time_machine_->then(set(key, value), [](std::future<SetResult>&& set_result) -> CASResult {
                return {call_get(std::move(set_result)).version, true};
            });

        auto path = get_path(key);

        if (!version) {
            return this->time_machine_->then(create(key, value),
                                             [this, path, value](std::future<void>&& res) -> CASResult {
                                                 try {
                                                     call_get(std::move(res));
                                                     return {0, true};
                                                 } catch (EntryExists&) {
                                                     return call_get(this->time_machine_->then(
                                                         client_.set(path, from_string(value), zk::version(0)),
                                                         [](std::future<zk::set_result>&& result) -> CASResult {
                                                             try {
                                                                 return {static_cast<int64_t>(
                                                                         call_get(std::move(result)).stat()
                                                                                                    .data_version
                                                                                                    .value), true};
                                                             } catch (zk::error& e) {
                                                                 if (e.code() == zk::error_code::version_mismatch) {
                                                                     // TODO: return real key's version instead of -1
                                                                     return {-1, false};
                                                                 }
                                                                 throw e;
                                                             }
                                                         }));
                                                 }
                                             });
        }
        return this->time_machine_->then(
            client_.set(get_path(key), from_string(value), zk::version(version)),
            [version](std::future<zk::set_result>&& result) -> CASResult {
                try {
                    return {static_cast<int64_t>(call_get(std::move(result)).stat().data_version.value), true};
                } catch (zk::error& e) {
                    if (e.code() == zk::error_code::version_mismatch) {
                        // TODO: return real key's version instead of -1
                        return {-1, false};
                    }
                    throw e;
                }
            });
    }

    std::future<GetResult> get(const std::string& key) const
    {
        return this->time_machine_->then(
            client_.get(get_path(key)),
            [](std::future<zk::get_result>&& result) -> GetResult {
                auto res = call_get(std::move(result));
                return {res.stat().data_version.value, to_string(res.data())};
            }
        );
    }

    std::future<void> erase(const std::string& key, int64_t version)
    {
        auto path = get_path(key);

        if (version < int64_t(0))
            return this->time_machine_->then(client_.erase(path), call_get<void>);
        return this->time_machine_->then(client_.erase(path, zk::version(version)), call_get<void>);
    }

    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        zk::multi_op trn;

        for (const auto& op_ptr : transaction) {
            switch (op_ptr->type) {
                case op::op_type::CREATE: {
                    auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());
                    trn.push_back(zk::op::create(get_path(create_op_ptr->key), from_string(create_op_ptr->value)));
                    break;
                }
                case op::op_type::CHECK: {
                    auto check_op_ptr = dynamic_cast<op::Check*>(op_ptr.get());
                    trn.push_back(zk::op::check(get_path(check_op_ptr->key), zk::version(check_op_ptr->version)));
                    break;
                }
                case op::op_type::SET: {
                    auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());
                    trn.push_back(zk::op::set(get_path(set_op_ptr->key), from_string(set_op_ptr->value)));
                    break;
                }
                case op::op_type::ERASE: {
                    auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());
                    trn.push_back(zk::op::erase(get_path(erase_op_ptr->key), zk::version(erase_op_ptr->version)));
                    break;
                }
                default:
                    __builtin_unreachable();
            }
        };

        return this->time_machine_->then(client_.commit(trn), [](std::future<zk::multi_result>&& multi_res) {
            TransactionResult result;

            auto multi_res_unwrapped = call_get(std::move(multi_res));

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
                    default:
                        __builtin_unreachable();
                }
            }

            return result;
        });
    }
};


template <typename TimeMachine>
const std::string ZKClient<TimeMachine>::KV_STORAGE_ZNODE = "/__KV__";
