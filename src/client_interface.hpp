#pragma once

#include <future>

#include "time_machine.hpp"
#include "operation.hpp"
#include "result.hpp"
#include "util.hpp"



class Client {
protected:
    using ThreadPool = time_machine::ThreadPool<>;
    std::shared_ptr <ThreadPool> thread_pool_;
    std::string address_;

public:
    Client(const std::string&, std::shared_ptr <ThreadPool> time_machine = nullptr)
        : thread_pool_(time_machine == nullptr ? std::make_shared<ThreadPool>() : std::move(time_machine))
    {}

    Client() = default;

    virtual
    ~Client() = default;

    std::string address() const
    {
        return address_;
    }

    virtual
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false) = 0;

    virtual
    std::future <ExistsResult> exists(const std::string& key) const = 0;

    virtual
    std::future <SetResult> set(const std::string& key, const std::string& value) = 0;

    virtual
    std::future <CASResult> cas(const std::string& key, const std::string& value, int64_t version = -1) = 0;

    virtual
    std::future <GetResult> get(const std::string& key) const = 0;

    virtual
    std::future<void> erase(const std::string& key, int64_t version = -1) = 0;

//    virtual
//    std::future<WatchResult> watch(const std::string& key) = 0;

    virtual
    std::future <TransactionResult> commit(const Transaction&) = 0;
};
