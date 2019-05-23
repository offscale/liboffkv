#pragma once

#include <future>

#include "time_machine.hpp"
#include "operation.hpp"
#include "result.hpp"
#include "util.hpp"
#include "key.hpp"



class Client {
protected:
    using ThreadPool = time_machine::ThreadPool<>;
    std::shared_ptr<ThreadPool> thread_pool_;
    std::string address_;
    std::string prefix_;

public:
    Client(std::shared_ptr<ThreadPool> time_machine = nullptr)
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
    std::future<ExistsResult> exists(const std::string& key, bool watch = false) = 0;

    virtual
    std::future<ChildrenResult> get_children(const std::string& key, bool watch = false) = 0;

    virtual
    std::future<SetResult> set(const std::string& key, const std::string& value) = 0;

    virtual
    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) = 0;

    virtual
    std::future<GetResult> get(const std::string& key, bool watch = false) = 0;

    virtual
    std::future<void> erase(const std::string& key, uint64_t version = 0) = 0;

    virtual
    std::future<TransactionResult> commit(const Transaction&) = 0;
};
