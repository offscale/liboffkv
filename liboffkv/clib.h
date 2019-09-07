#ifndef liboffkv_h_
#define liboffkv_h_

#include <stddef.h>
#include <stdint.h>

// flags
enum {
    OFFKV_LEASE = 1 << 0,
    OFFKV_WATCH = 1 << 1,
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

// Filled and destroyed (with /offkv_exists_result_free()/) by the library.
typedef struct {
    int64_t version;
    void *watch;
} offkv_ExistsResult;

// Filled and destroyed (with /offkv_get_result_free()/) by the library.
typedef struct {
    char *value;
    size_t nvalue;
    int64_t version;
    void *watch;
} offkv_GetResult;

// Filled and destroyed (with /offkv_children_free()/) by the library.
typedef struct {
    char **keys;
    size_t nkeys;
    int64_t version;
    void *watch;
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
    int64_t version;
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

// On error, returns /NULL/ and writes to /errcode/, unless it is /NULL/.
offkv_Handle
offkv_open(const char * /*addr*/, const char * /*prefix*/, int * /*errcode*/);

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
// On success, returns the version of the existing node.
offkv_ExistsResult
offkv_exists(offkv_Handle, const char * /*key*/, int /*flags*/);

// On error, returns /offkv_GetResult/ with a negative /version/ field; it should not be freed in
// this case.
offkv_GetResult
offkv_get(offkv_Handle, const char * /*key*/, int /*flags*/);

// On error, returns /offkv_Chilren/ with a negative /version/ field; it should not be freed in this
// case.
offkv_Children
offkv_children(offkv_Handle, const char * /*key*/, int /*flags*/);

// On error, returns negative value.
// On success, returns 0.
//
// Always consumes the watch.
int
offkv_watch(void * /*watch*/);

void
offkv_watch_drop(void * /*watch*/);

// On error, returns negative value.
// On success, returns 0.
int
offkv_erase(offkv_Handle, const char * /*key*/, int64_t /*ver*/);

// On error, returns negative value.
// On success, returns the version of the node.
int64_t
offkv_cas(offkv_Handle, const char * /*key*/, const char * /*value*/, int64_t /*ver*/);

// On a non-transaction error (for example, if a key is invalid), its code is returned; contents of
// /*out/ is undefined.
//
// On a transaction error, /OFFKV_ETXN/ is returned and /out->failed_op/ is filled; contents of the
// other fields of /*out/ is undefined.
//
// On success, 0 is returned and /*out/ is filled; /out->failed_op/ is set to /(size_t) -1/.
int
offkv_commit(offkv_Handle,
             const offkv_TxnCheck * /*checks*/, size_t /*nchecks*/,
             const offkv_TxnOp * /*ops*/, size_t /*nops*/,
             offkv_TxnResult * /*out*/);

// Frees a value returned from /offkv_exists()/.
void
offkv_exists_result_free(offkv_ExistsResult);

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

#endif
