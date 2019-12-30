#pragma once

#include <stddef.h>
#include <stdint.h>

// flags
enum {
    OFFKV_LEASE = 1 << 0,
};

// errors
enum {
    OFFKV_EADDR  = -1,
    OFFKV_EKEY   = -2,
    OFFKV_ENOENT = -3,
    OFFKV_EEXIST = -4,
    OFFKV_EEPHEM = -5,
    OFFKV_ECONN  = -6,
    OFFKV_ETXN   = -7,
    OFFKV_ESRV   = -8,
    OFFKV_ENOMEM = -9,
};

// transaction operations
enum {
    OFFKV_OP_CREATE,
    OFFKV_OP_SET,
    OFFKV_OP_ERASE,
};

typedef void *offkv_Handle;
typedef void *offkv_Watch;

// Filled and destroyed (with /offkv_get_result_free()/) by the library.
typedef struct {
    char *value;
    size_t nvalue;
    int64_t version;
} offkv_GetResult;

// Filled and destroyed (with /offkv_children_free()/) by the library.
typedef struct {
    char **keys;
    size_t nkeys;
    int errcode;
} offkv_Children;

// Filled and (possibly) destroyed by user.
typedef struct {
    const char *key;
    int64_t version;
} offkv_TxnCheck;

// Filled and (possibly) destroyed by user.
typedef struct {
    int op;
    int flags;
    const char *key;
    const char *value;
    size_t nvalue;
} offkv_TxnOp;

typedef struct {
    int op;
    int64_t version;
} offkv_TxnOpResult;

// Filled and destroyed (with /offkv_txn_result_free()/) by the library.
typedef struct {
    offkv_TxnOpResult *results;
    size_t nresults;
    size_t failed_op;
} offkv_TxnResult;

// Returns a pointer to a static string.
const char *
offkv_error_descr(int /*errcode*/);

// On error, returns /NULL/ and writes to /p_errcode/, unless it is /NULL/.
offkv_Handle
offkv_open(const char * /*url*/, const char * /*prefix*/, int * /*p_errcode*/);

// On error, returns negative value.
// On success, returns the version of the created node.
int64_t
offkv_create(
    offkv_Handle,
    const char * /*key*/,
    const char * /*value*/,
    size_t /*nvalue*/,
    int /*flags*/);

// On error, returns negative value.
// On success, returns the version of the created node.
int64_t
offkv_set(
    offkv_Handle,
    const char * /*key*/,
    const char * /*value*/,
    size_t /*nvalue*/);

// If /p_watch/ is not NULL, a new watch is created and written into it.
//
// On error, returns negative value.
// On success, returns the version of the existing node or 0.
int64_t
offkv_exists(offkv_Handle, const char * /*key*/, offkv_Watch * /*p_watch*/);

// If /p_watch/ is not NULL, a new watch is created and written into it.
//
// On error, returns /offkv_GetResult/ with a negative /version/ field; it may not be freed in this
// case.
offkv_GetResult
offkv_get(offkv_Handle, const char * /*key*/, offkv_Watch * /*p_watch*/);

// If /p_watch/ is not NULL, a new watch is created and written into it.
//
// On error, returns /offkv_Chilren/ with a negative /errcode/ field; it may not be freed in this
// case.
offkv_Children
offkv_children(offkv_Handle, const char * /*key*/, offkv_Watch * /*p_watch*/);

// On error, returns negative value.
// On success, returns 0.
//
// Never consumes the watch; you still have to call /offkv_watch_drop/ on it.
int
offkv_watch(offkv_Watch /*watch*/);

// Frees a watch.
void
offkv_watch_drop(offkv_Watch /*watch*/);

// On error, returns negative value.
// On success, returns 0.
int
offkv_erase(offkv_Handle, const char * /*key*/, int64_t /*version*/);

// On error, returns negative value.
// If compare-and-swap succeeded, returns the new version of the node (positive).
// If compare-and-swap failed, returns 0.
int64_t
offkv_cas(
    offkv_Handle,
    const char * /*key*/,
    const char * /*value*/,
    size_t /*nvalue*/,
    int64_t /*version*/);

// On a non-transaction error (for example, if a key is invalid), its code is returned and
// /p_results/ is not written to.
//
// On a transaction error, /OFFKV_ETXN/ is returned and, if /p_results/ is not NULL:
//   1. /p_results->failed_op/ is filled with the index of the failed operation;
//   2. /*p_results/ may not be freed.
//
// On success, 0 is returned and, if /p_results/ is not NULL, /*p_results/ is filled (and
// /p_results->failed_op/ is set to /(size_t) -1/).
int
offkv_commit(
    offkv_Handle,
    const offkv_TxnCheck * /*checks*/, size_t /*nchecks*/,
    const offkv_TxnOp * /*ops*/, size_t /*nops*/,
    offkv_TxnResult * /*p_results*/);

// Frees a value returned from /offkv_get()/.
void
offkv_get_result_free(offkv_GetResult);

// Frees a value returned from /offkv_children()/.
void
offkv_children_free(offkv_Children);

// Frees a value written to by /offkv_commit()/.
void
offkv_txn_result_free(offkv_TxnResult);

// Closes a handle returned from /offkv_open()/.
void
offkv_close(offkv_Handle);
