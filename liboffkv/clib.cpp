#include <liboffkv/liboffkv.hpp>
#include <new>
#include <string>

#include <stddef.h>
#include <stdint.h>

extern "C" {
#include "clib.h"
}

const char *offkv_error_descr(int errcode)
{
    switch (errcode) {
    case OFFKV_EADDR:
        return "invalid address";
    case OFFKV_EKEY:
        return "invalid key";
    case OFFKV_ENOENT:
        return "no entry";
    case OFFKV_EEXIST:
        return "entry exists";
    case OFFKV_EEPHEM:
        return "attempt to create a child of ephemeral node";
    case OFFKV_ECONN:
        return "connection loss";
    case OFFKV_ETXN:
        return "transaction failed";
    case OFFKV_ESRV:
        return "service error";
    case OFFKV_ENOMEM:
        return "out of memory";
    default:
        return nullptr;
    }
}

static int to_errcode(const std::exception &e)
{
    if (dynamic_cast<const liboffkv::InvalidAddress *>(&e))
        return OFFKV_EADDR;
    if (dynamic_cast<const liboffkv::InvalidKey *>(&e))
        return OFFKV_EKEY;
    if (dynamic_cast<const liboffkv::NoEntry *>(&e))
        return OFFKV_ENOENT;
    if (dynamic_cast<const liboffkv::EntryExists *>(&e))
        return OFFKV_EEXIST;
    if (dynamic_cast<const liboffkv::NoChildrenForEphemeral *>(&e))
        return OFFKV_EEPHEM;
    if (dynamic_cast<const liboffkv::ConnectionLoss *>(&e))
        return OFFKV_ECONN;
    if (dynamic_cast<const liboffkv::TxnFailed *>(&e))
        return OFFKV_ETXN;
    if (dynamic_cast<const liboffkv::ServiceError *>(&e))
        return OFFKV_ESRV;
    if (dynamic_cast<const std::bad_alloc *>(&e))
        return OFFKV_ENOMEM;
    throw e;
}

offkv_Handle offkv_open(const char *addr, const char *prefix, int *errcode)
{
    try {
        std::unique_ptr<liboffkv::Client> client = liboffkv::open(addr, prefix);
        return reinterpret_cast<offkv_Handle>(client.release());
    } catch (const std::exception &e) {
        if (errcode)
            *errcode = to_errcode(e);
        return nullptr;
    }
}

int64_t offkv_create(offkv_Handle h, const char *key, const char *value, size_t nvalue, int flags)
{
    try {
        return reinterpret_cast<liboffkv::Client *>(h)->create(
            key,
            std::string(value, nvalue),
            flags & OFFKV_LEASE
        );
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

void offkv_close(offkv_Handle h)
{
    delete reinterpret_cast<liboffkv::Client *>(h);
}
