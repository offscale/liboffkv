#pragma once

#include "config.h"

#include <exception>
#include <future>
#include <string.h>



#ifdef ENABLE_ZK

#include <zk/error.hpp>


#define liboffkv_catch_zk \
    catch (zk::error& e) {\
        switch (e.code()) {\
            case zk::error_code::no_entry:\
                throw NoEntry{};\
            case zk::error_code::entry_exists:\
                throw EntryExists{};\
            case zk::error_code::connection_loss:\
                throw ConnectionLoss{};\
            case zk::error_code::no_children_for_ephemerals:\
                throw NoChildrenForEphemeral{};\
            case zk::error_code::version_mismatch:\
            case zk::error_code::not_empty:\
                std::rethrow_exception(std::current_exception());\
            default: \
                std::rethrow_exception(std::current_exception());\
        }\
    }
#else
#define liboffkv_catch_zk
#endif


#ifdef ENABLE_CONSUL

#include <ppconsul/error.h>
#include <ppconsul/kv.h>


#define liboffkv_catch_consul \
    catch (ppconsul::BadStatus& e) {\
        throw ServiceException(e.what());\
    } catch (ppconsul::kv::UpdateError& e) {\
        throw /* TODO: use e.key(), e.what() */ e;\
    }
#else
#define liboffkv_catch_consul
#endif


// used to avoid compilation errors when zk and consul are disabled
#define liboffkv_catch_default \
    catch (...) {\
        std::rethrow_exception(std::current_exception());\
    }\


#define liboffkv_catch liboffkv_catch_zk liboffkv_catch_consul liboffkv_catch_default



namespace liboffkv {

class InvalidAddress : public std::exception {
private:
    std::string msg_;

public:
    InvalidAddress(std::string description)
        : msg_(std::string("invalid address: ") + description)
    {}

    virtual
    const char* what() const noexcept override
    {
        return msg_.c_str();
    }
};


class InvalidKey : public std::exception {
private:
    std::string msg_;

public:
    InvalidKey(std::string description)
        : msg_(std::string("invalid key: ") + description)
    {}

    virtual
    const char* what() const noexcept override
    {
        return msg_.c_str();
    }
};


class NoEntry : public std::exception {
public:
    virtual
    const char* what() const noexcept override
    {
        return "no entry";
    }
};


class EntryExists : public std::exception {
public:
    virtual
    const char* what() const noexcept override
    {
        return "entry exists";
    }
};


class NoChildrenForEphemeral : public std::exception {
public:
    virtual
    const char* what() const noexcept override
    {
        return "attempt to create children of ephemeral node";
    }
};

class ConnectionLoss : public std::exception {
public:
    virtual
    const char* what() const noexcept override
    {
        return "connection loss";
    }
};

class TransactionFailed : public std::exception {
private:
    size_t op_index_;
    std::string msg_;

public:
    TransactionFailed(size_t index) noexcept
        : op_index_(index), msg_(std::string("transaction failed on ") + std::to_string(op_index_) + " operation")
    {}

    virtual
    const char* what() const noexcept override
    {
        return msg_.c_str();
    }

    size_t failed_operation_index() const
    {
        return op_index_;
    }
};


class ServiceException : public std::runtime_error {
public:
    ServiceException(const std::string& arg)
        : runtime_error(arg)
    {}
};

} // namespace liboffkv
