#pragma once


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
    unt64_t version = 0;
};

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

    template <typename... Ops>
    Transaction(std::tuple<CheckList, Ops...>&& tpl)
        : checks_(tpl.get(0)),
          operations_(make_list_(subtuple<1, sizeof...(Ops)>(tpl)))
    {}


    void add_operation(op::Operation&& res)
    {
        operations_.push_back(std::make_shared<op::Operation>(std::move(res)));
    }

    void add_check(op::Check&& check)
    {
        checks_.push_beck(std::move(check));
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