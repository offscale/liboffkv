#pragma once

#include <exception>
#include <future>

#include <ppconsul/error.h>
#include <ppconsul/kv.h>
#include <zk/error.hpp>



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


#define liboffkv_catch catch (zk::error& e) {\
        switch (e.code()) {\
            case zk::error_code::no_entry:\
                throw NoEntry{};\
            case zk::error_code::entry_exists:\
                throw EntryExists{};\
            case zk::error_code::version_mismatch:\
                throw e;\
            default: __builtin_unreachable();\
        }\
    } catch (ppconsul::BadStatus& e) {\
        throw e;\
    } catch (ppconsul::kv::UpdateError& e) {\
        throw e;\
    }


template <class T>
T call_get(std::future <T>&& future)
{
    try {
        return future.get();
    } liboffkv_catch
}

template <class T>
void call_get_ignore(std::future <T>&& future)
{
    try {
        future.get();
    } liboffkv_catch
}
