#pragma once

#include "config.h"

#include <exception>
#include <future>



#ifdef ENABLE_ZK

#include <zk/error.hpp>



#define liboffkv_catch_zk \
    catch (zk::error& e) {\
        switch (e.code()) {\
            case zk::error_code::no_entry:\
                throw NoEntry{};\
            case zk::error_code::entry_exists:\
                throw EntryExists{};\
            case zk::error_code::version_mismatch:\
                throw e;\
            default: \
                throw e; \
            /*default: __builtin_unreachable();\*/ \
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
        throw e;\
    } catch (ppconsul::kv::UpdateError& e) {\
        throw e;\
    }
#else
#define liboffkv_catch_consul
#endif

// used to avoid compilation errors when zk and consul are disabled
#define liboffkv_catch_default \
    catch (...) {\
        std::rethrow_exception(std::current_exception());\
    }\


class InvalidAddress : public std::exception {
public:
    virtual const char* what() const noexcept override
    {
        return "invalid address";
    }
};


class InvalidKey : public std::exception {
public:
    virtual const char* what() const noexcept override
    {
        return "invalid key";
    }
};


class NoEntry : public std::exception {
public:
    virtual const char* what() const noexcept override
    {
        return "no entry";
    }
};


class EntryExists : public std::exception {
    virtual const char* what() const noexcept override
    {
        return "entry exists";
    }
};


#define liboffkv_catch liboffkv_catch_zk liboffkv_catch_consul liboffkv_catch_default


template <class T>
T call_get(std::future<T>&& future)
{
    try {
        return future.get();
    } liboffkv_catch
}

template <class T>
void call_get_ignore(std::future<T>&& future)
{
    try {
        future.get();
    } liboffkv_catch
}

template <class T>
void call_get_ignore_noexcept(std::future<T>&& future)
{
    try {
        future.get();
    } catch (...) {}
}
