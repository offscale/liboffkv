#pragma once

#include <vector>
#include <string>
#include <memory>
#include <variant>
#include <utility>
#include <cstdint>
#include "key.hpp"

namespace liboffkv {

class WatchHandle
{
public:
    virtual void wait() = 0;
    virtual ~WatchHandle() = default;
};

struct ExistsResult
{
    int64_t version;
    std::unique_ptr<WatchHandle> watch;

    operator bool() const { return version != 0; }
};

struct ChildrenResult
{
    std::vector<std::string> children;
    std::unique_ptr<WatchHandle> watch;
};

struct GetResult
{
    int64_t version;
    std::string value;
    std::unique_ptr<WatchHandle> watch;
};

struct CasResult
{
    int64_t version;

    operator bool() const { return version != 0; }
};

struct TxnCheck
{
    Key key;
    int64_t version;

    TxnCheck(Key key_, int64_t version_)
        : key(std::move(key_))
        , version{version_}
    {}
};

struct TxnOpCreate
{
    Key key;
    std::string value;
    bool lease;

    TxnOpCreate(Key key_, std::string value_, bool lease_ = false)
        : key(std::move(key_))
        , value(std::move(value_))
        , lease{lease_}
    {}
};

struct TxnOpSet
{
    Key key;
    std::string value;

    TxnOpSet(Key key_, std::string value_)
        : key(std::move(key_))
        , value(std::move(value_))
    {}
};

struct TxnOpErase
{
    Key key;

    explicit TxnOpErase(Key key_)
        : key(std::move(key_))
    {}
};

using TxnOp = std::variant<TxnOpCreate, TxnOpSet, TxnOpErase>;

struct TxnOpResult
{
    enum class Kind
    {
        CREATE,
        SET,
    };

    Kind kind;
    int64_t version;
};


using TransactionResult = std::vector<TxnOpResult>;

struct Transaction {
    std::vector<TxnCheck> checks;
    std::vector<TxnOp> ops;
};


class Client
{
protected:
    Path prefix_;

public:
    explicit Client(Path prefix)
        : prefix_{std::move(prefix)}
    {}

    virtual int64_t create(const Key &key, const std::string &value, bool lease = false) = 0;

    virtual ExistsResult exists(const Key &key, bool watch = false) = 0;

    virtual ChildrenResult get_children(const Key &key, bool watch = false) = 0;

    virtual int64_t set(const Key &key, const std::string &value) = 0;

    virtual GetResult get(const Key &key, bool watch = false) = 0;

    virtual CasResult cas(const Key &key, const std::string &value, int64_t version = 0) = 0;

    virtual void erase(const Key &key, int64_t version = 0) = 0;

    virtual TransactionResult commit(const Transaction&) = 0;

    virtual ~Client() = default;
};

} // namespace liboffkv
