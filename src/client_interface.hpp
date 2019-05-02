#pragma once

#include <future>


#include "operation.hpp"
#include "result.hpp"
#include "util.hpp"



template <typename TimeMachine>
class Client {
protected:
    std::shared_ptr<TimeMachine> time_machine_;

public:
    Client(const std::string&, std::shared_ptr<TimeMachine> time_machine = nullptr)
        : time_machine_(time_machine == nullptr ? std::make_shared<TimeMachine>() : std::move(time_machine))
    {}

    Client(Client&& another)
        : time_machine_(std::move(another.time_machine_))
    {}

    Client() = default;

    virtual
    ~Client() = default;


    virtual
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false) = 0;

    virtual
    std::future<ExistsResult> exists(const std::string& key) const = 0;

    virtual
    std::future<SetResult> set(const std::string& key, const std::string& value) = 0;

    virtual
    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version = 0) = 0;

    virtual
    std::future<GetResult> get(const std::string& key) const = 0;

    virtual
    std::future<void> erase(const std::string& key, int64_t version = 0) = 0;

//    virtual
//    std::future<WatchResult> watch(const std::string& key) = 0;

    virtual
    std::future<TransactionResult> commit(const Transaction&) = 0;
};
