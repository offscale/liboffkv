#include <liboffkv/liboffkv.hpp>
#include <new>
#include <string>
#include <algorithm>
#include <vector>

#include <stddef.h>
#include <stdint.h>

#if __GNUC__ >= 2
#   define UNREACHABLE() __builtin_unreachable()
#elif _MSC_VER >= 1100
#   define UNREACHABLE() __assume(false)
#else
#   define UNREACHABLE() void()
#endif

extern "C" {
#include "clib.h"
}

static inline offkv_Handle wrap_client(std::unique_ptr<liboffkv::Client> &&p)
{
    return reinterpret_cast<offkv_Handle>(p.release());
}

static inline offkv_Watch wrap_watch(std::unique_ptr<liboffkv::WatchHandle> &&p)
{
    return reinterpret_cast<offkv_Watch>(p.release());
}

static inline liboffkv::Client *unwrap_client(offkv_Handle h)
{
    return reinterpret_cast<liboffkv::Client *>(h);
}

static inline liboffkv::WatchHandle *unwrap_watch(offkv_Watch w)
{
    return reinterpret_cast<liboffkv::WatchHandle *>(w);
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
    else if (dynamic_cast<const liboffkv::InvalidKey *>(&e))
        return OFFKV_EKEY;
    else if (dynamic_cast<const liboffkv::NoEntry *>(&e))
        return OFFKV_ENOENT;
    else if (dynamic_cast<const liboffkv::EntryExists *>(&e))
        return OFFKV_EEXIST;
    else if (dynamic_cast<const liboffkv::NoChildrenForEphemeral *>(&e))
        return OFFKV_EEPHEM;
    else if (dynamic_cast<const liboffkv::ConnectionLoss *>(&e))
        return OFFKV_ECONN;
    else if (dynamic_cast<const liboffkv::TxnFailed *>(&e))
        return OFFKV_ETXN;
    else if (dynamic_cast<const liboffkv::ServiceError *>(&e))
        return OFFKV_ESRV;
    else if (dynamic_cast<const std::bad_alloc *>(&e))
        return OFFKV_ENOMEM;
    throw e;
}

static char *dup_string(const std::string &s)
{
    char *r = new char[s.size() + 1];
    const auto data = s.data();
    std::copy(data, data + s.size() + 1, r);
    return r;
}

static void free_string(char *p)
{
    delete[] p;
}

static void free_strings(char **strings, size_t nstrings)
{
    for (size_t i = 0; i < nstrings; ++i)
        free_string(strings[i]);
    delete[] strings;
}

static char **dup_strings(const std::vector<std::string> &strings)
{
    char **r = new char *[strings.size()];
    for (size_t i = 0; i < strings.size(); ++i)
        try {
            r[i] = dup_string(strings[i]);
        } catch (...) {
            free_strings(r, i);
            throw;
        }
    return r;
}

offkv_Handle offkv_open(const char *url, const char *prefix, int *p_errcode)
{
    try {
        return wrap_client(liboffkv::open(url, prefix));
    } catch (const std::exception &e) {
        const int errcode = to_errcode(e);
        if (p_errcode)
            *p_errcode = errcode;
        return nullptr;
    }
}

int64_t offkv_create(offkv_Handle h, const char *key, const char *value, size_t nvalue, int flags)
{
    try {
        return unwrap_client(h)->create(key, std::string(value, nvalue), flags & OFFKV_LEASE);
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

int64_t offkv_set(offkv_Handle h, const char *key, const char *value, size_t nvalue)
{
    try {
        return unwrap_client(h)->set(key, std::string(value, nvalue));
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

int64_t offkv_exists(offkv_Handle h, const char *key, offkv_Watch *p_watch)
{
    try {
        auto r = unwrap_client(h)->exists(key, !!p_watch);
        if (p_watch)
            *p_watch = wrap_watch(std::move(r.watch));
        return r.version;
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

offkv_GetResult offkv_get(offkv_Handle h, const char *key, offkv_Watch *p_watch)
{
    try {
        auto r = unwrap_client(h)->get(key, !!p_watch);
        if (p_watch)
            *p_watch = wrap_watch(std::move(r.watch));
        return {
            dup_string(r.value),
            r.value.size(),
            r.version,
        };
    } catch (const std::exception &e) {
        return {
            nullptr,
            0,
            static_cast<int64_t>(to_errcode(e)),
        };
    }
}

offkv_Children offkv_children(offkv_Handle h, const char *key, offkv_Watch *p_watch)
{
    try {
        auto r = unwrap_client(h)->get_children(key, !!p_watch);
        if (p_watch)
            *p_watch = wrap_watch(std::move(r.watch));
        return {
            dup_strings(r.children),
            r.children.size(),
            0,
        };
    } catch (const std::exception &e) {
        return {
            nullptr,
            0,
            to_errcode(e),
        };
    }
}

void offkv_children_free(offkv_Children c)
{
    free_strings(c.keys, c.nkeys);
}

void offkv_get_result_free(offkv_GetResult r)
{
    free_string(r.value);
}

int offkv_watch(offkv_Watch w)
{
    try {
        unwrap_watch(w)->wait();
        return 0;
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

void offkv_watch_drop(offkv_Watch w)
{
    delete unwrap_watch(w);
}

int offkv_erase(offkv_Handle h, const char *key, int64_t version)
{
    try {
        unwrap_client(h)->erase(key, version);
        return 0;
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

int64_t offkv_cas(
    offkv_Handle h,
    const char *key,
    const char *value,
    size_t nvalue,
    int64_t version)
{
    try {
        auto r = unwrap_client(h)->cas(key, std::string(value, nvalue), version);
        return r ? r.version : 0;
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

static offkv_TxnOpResult *dup_txn_results(const std::vector<liboffkv::TxnOpResult> &data)
{
    offkv_TxnOpResult *r = new offkv_TxnOpResult[data.size()];
    std::transform(data.begin(), data.end(), r, [](liboffkv::TxnOpResult arg) -> offkv_TxnOpResult {
        switch (arg.kind) {
        case liboffkv::TxnOpResult::Kind::CREATE:
            return {OFFKV_OP_CREATE, arg.version};
        case liboffkv::TxnOpResult::Kind::SET:
            return {OFFKV_OP_SET, arg.version};
        }
        UNREACHABLE();
    });
    return r;
}

static void free_txn_results(offkv_TxnOpResult *p)
{
    delete[] p;
}

int
offkv_commit(
    offkv_Handle h,
    const offkv_TxnCheck *checks, size_t nchecks,
    const offkv_TxnOp *ops, size_t nops,
    offkv_TxnResult *p_result)
{
    try {
        std::vector<liboffkv::TxnCheck> checks_vec;
        for (size_t i = 0; i < nchecks; ++i)
            checks_vec.emplace_back(checks[i].key, checks[i].version);

        std::vector<liboffkv::TxnOp> ops_vec;
        for (size_t i = 0; i < nops; ++i)
            switch (ops[i].op) {
            case OFFKV_OP_CREATE:
                ops_vec.push_back(liboffkv::TxnOpCreate(
                    ops[i].key,
                    std::string(ops[i].value, ops[i].nvalue),
                    ops[i].flags & OFFKV_LEASE
                ));
                break;
            case OFFKV_OP_SET:
                ops_vec.push_back(liboffkv::TxnOpSet(
                    ops[i].key,
                    std::string(ops[i].value, ops[i].nvalue)
                ));
                break;
            case OFFKV_OP_ERASE:
                ops_vec.push_back(liboffkv::TxnOpErase(ops[i].key));
                break;
            default:
                UNREACHABLE();
            }

        auto r = unwrap_client(h)->commit({checks_vec, ops_vec});
        if (p_result)
            *p_result = {dup_txn_results(r), r.size(), static_cast<size_t>(-1)};
        return 0;

    } catch (const liboffkv::TxnFailed &e) {
        if (p_result)
            *p_result = {nullptr, 0, e.failed_op()};
        return OFFKV_ETXN;
    } catch (const std::exception &e) {
        return to_errcode(e);
    }
}

void offkv_txn_result_free(offkv_TxnResult r)
{
    free_txn_results(r.results);
}

void offkv_close(offkv_Handle h)
{
    delete unwrap_client(h);
}
