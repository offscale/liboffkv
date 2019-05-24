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

    Client(std::shared_ptr<ThreadPool> time_machine = nullptr)
        : thread_pool_(time_machine == nullptr ? std::make_shared<ThreadPool>() : std::move(time_machine))
    {}

public:

    virtual
    ~Client() = default;

    std::string address() const
    {
        return address_;
    }


    /*
     * Create the key.
     * Throw EntryExists if the key already exists.
     * Throw NoEntry if preceding entry does not exist.
     */
    virtual
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false) = 0;


    /*
     * Check if the key exists.
     */
    virtual
    std::future<ExistsResult> exists(const std::string& key, bool watch = false) = 0;


    /*
     * Get a list of key's children
     * Throw NoEntry, if the key does not exist.
     *
     * Assign watch to the key if <watch> is true
     *
     *
     * get_children("/foo") -> {"/foo/bar", "foo/baz"}
     * get_children("/foo/bar") -> {"/foo/bar/var"}
     *
     */
    virtual
    std::future<ChildrenResult> get_children(const std::string& key, bool watch = false) = 0;


    /*
     * Set the value and to the key.
     * Create the key if it doesn’t exist.
     * Throw NoEntry if preceding entry does not exist.
     *
     * Assign watch to the key if <watch> is true
     */
    virtual
    std::future<SetResult> set(const std::string& key, const std::string& value) = 0;


    /*
     * Compare and set operation.
     *
     * If the key does not exist and version equals 0 create it.
     * Throw NoEntry if preceding entry does not exist.
     *
     * If the key exists and its version equals specified version update value.
     * Otherwise do nothing.
     */
    virtual
    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) = 0;


    /*
     * Throw NoEntry if the key does not exist.
     */
    virtual
    std::future<GetResult> get(const std::string& key, bool watch = false) = 0;


    /*
     * Delete the key, if its version is equal to specified version or specified version equals 0.
     * Throw NoEntry, if the key does not exist.
     *
     * Assign watch to the key if <watch> is true
     */
    virtual
    std::future<void> erase(const std::string& key, uint64_t version = 0) = 0;


    /*
     * Perform transaction.
     * Throw TransactionFailed if it fails
     */
    virtual
    std::future<TransactionResult> commit(const Transaction&) = 0;
};
