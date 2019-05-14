#pragma once

#include "util.hpp"


namespace op {

enum class op_type {
    CREATE,
    SET,
    ERASE,
};


struct Operation {
    op_type type;
    std::string key;

    explicit Operation(op_type type, std::string key)
        : type(type), key(key)
    {}

    virtual ~Operation() = default;
};


struct Create : Operation {
    std::string value;

    Create(const std::string& key, const std::string& value)
        : Operation(op_type::CREATE, key), value(value)
    {}
};


struct Set : Operation {
    std::string value;

    Set(const std::string& key, const std::string& value)
        : Operation(op_type::SET, key), value(value)
    {}
};


struct Erase : Operation {
    int64_t version;

    Erase(const std::string& key, int64_t version)
        : Operation(op_type::ERASE, key), version(version)
    {}
};


struct Check {
    std::string key;
    uint64_t version = 0;

    Check(const std::string& key, uint64_t version)
        : key(key), version(version)
    {}
};


std::shared_ptr<op::Create> create(const std::string& key, const std::string& value)
{
    return std::make_shared<op::Create>(key, value);
}

std::shared_ptr<op::Set> set(const std::string& key, const std::string& value)
{
    return std::make_shared<op::Set>(key, value);
}

std::shared_ptr<op::Erase> erase(const std::string& key, uint64_t version)
{
    return std::make_shared<op::Erase>(key, version);
}

} // namespace op



using CheckList = std::vector<op::Check>;
using OperationList = std::vector<std::shared_ptr<op::Operation>>;


class Transaction {
private:
    CheckList checks_;
    OperationList operations_;

public:
    Transaction()
    {}

    Transaction(const Transaction&) = default;

    Transaction(Transaction&&) = default;

    ~Transaction()
    {}

    Transaction(CheckList&& checks, OperationList&& ops)
        : checks_(std::move(checks)),
          operations_(std::move(ops))
    {}


    void add_operation(op::Operation&& res)
    {
        operations_.push_back(std::make_shared<op::Operation>(std::move(res)));
    }

    void add_check(op::Check&& check)
    {
        checks_.push_back(std::move(check));
    }

    const CheckList& checks() const
    {
        return checks_;
    }

    const OperationList& operations() const
    {
        return operations_;
    }
};
