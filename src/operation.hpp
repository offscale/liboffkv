#pragma once


namespace op {

enum class op_type {
    CREATE,
    CHECK,
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


struct Check : Operation {
    int64_t version;

    Check(const std::string& key, int64_t version)
        : Operation(op_type::CHECK, key), version(version)
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

} // namespace op


class Transaction {
private:
    std::vector<std::shared_ptr<op::Operation>> operations_;


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
    Transaction(std::tuple<Ops...>&& tpl)
        : operations_(make_list_(tpl))
    {}


    void push_back(op::Operation&& res)
    {
        operations_.push_back(std::make_shared<op::Operation>(res));
    }

    void pop_back()
    {
        operations_.pop_back();
    }

    auto begin() const
    {
        return operations_.begin();
    }

    auto end() const
    {
        return operations_.end();
    }
};