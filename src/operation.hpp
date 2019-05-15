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
    bool leased;

    Create(const std::string& key, const std::string& value, bool leased = false)
        : Operation(op_type::CREATE, key), value(value), leased(leased)
    {}
};


struct Set : Operation {
    std::string value;

    Set(const std::string& key, const std::string& value)
        : Operation(op_type::SET, key), value(value)
    {}
};


struct Erase : Operation {
    Erase(const std::string& key)
        : Operation(op_type::ERASE, key)
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

std::shared_ptr<op::Erase> erase(const std::string& key)
{
    return std::make_shared<op::Erase>(key);
}

} // namespace op



using CheckList = std::vector<op::Check>;
using OperationList = std::vector<std::shared_ptr<op::Operation>>;


class Transaction {
private:
    CheckList checks_;
    OperationList operations_;


    template <typename... Ops>
    static
    std::initializer_list<std::shared_ptr<op::Operation>> make_list_(std::tuple<Ops...>&& tpl)
    {
        return make_list_(std::move(tpl), std::tuple_size<decltype(tpl)>::value);
    }

    template <typename... Ops, size_t N, typename Indices = std::make_index_sequence<N>>
    static
    std::initializer_list<std::shared_ptr<op::Operation>> make_list_(std::tuple<Ops...>&& tpl,
                                                                     std::integral_constant<size_t, N>)
    {
        return make_list_(std::move(tpl), Indices{});
    }

    template <typename... Ops, size_t... indices>
    static
    std::initializer_list<std::shared_ptr<op::Operation>> make_list_(std::tuple<Ops...>&& tpl,
                                                                     std::index_sequence<indices...>)
    {
        return {std::make_shared<Ops>(tpl[indices])...};
    }

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
