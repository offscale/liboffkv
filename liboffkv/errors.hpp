#pragma once

#include <stddef.h>
#include <exception>
#include <string>
#include <utility>

namespace liboffkv {

class Error : public std::exception {};

class InvalidAddress : public Error
{
    std::string addr_;
public:
    InvalidAddress(std::string addr) : addr_(std::move(addr)) {}

    const char *what() const noexcept override { return addr_.c_str(); }
};

class InvalidKey : public Error
{
    std::string key_;
public:
    InvalidKey(std::string key) : key_(std::move(key)) {}

    const char *what() const noexcept override { return key_.c_str(); }
};

class NoEntry : public Error
{
public:
    const char *what() const noexcept override { return "no entry"; }
};

class EntryExists : public Error
{
public:
    const char *what() const noexcept override { return "entry exists"; }
};

class NoChildrenForEphemeral : public Error
{
public:
    const char *what() const noexcept override
    {
        return "attempt to create a child of ephemeral node";
    }
};

class ConnectionLoss : public Error
{
public:
    const char *what() const noexcept override { return "connection loss"; }
};

class TxnFailed : public Error
{
    size_t failed_op_;
    std::string what_;

public:
    TxnFailed(size_t failed_op)
        : failed_op_{failed_op}
        , what_(std::string("transaction failed on operation with index: ") +
                    std::to_string(failed_op))
    {}

    const char *what() const noexcept override { return what_.c_str(); }

    size_t failed_op() const { return failed_op_; }
};

class ServiceError : public Error
{
    std::string what_;
public:
    ServiceError(std::string what) : what_(std::move(what)) {}

    const char *what() const noexcept override { return what_.c_str(); }
};

} // namespace liboffkv
