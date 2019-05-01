#pragma once

#include <string>


#include "operation.hpp"



struct Result {
    int64_t version = 0;
};


struct CreateResult : Result {};


struct SetResult : Result {};


struct ExistsResult : Result {
    bool exists;

    explicit operator bool() const
    {
        return exists;
    }

    bool operator!() const
    {
        return !exists;
    }
};


struct GetResult : Result {
    std::string value;
};


struct CASResult : Result {
    bool success;

    explicit operator bool() const
    {
        return success;
    }

    bool operator!() const
    {
        return !success;
    }
};


class TransactionResult {
private:
    struct OperationResult {
        op::op_type type;
        std::shared_ptr<Result> result;

        OperationResult(op::op_type type, std::shared_ptr<Result> resul)
            : type(type), result(result)
        {}
    };

    std::vector<OperationResult> op_results_;

public:
    TransactionResult()
    {}

    template <typename... Args>
    TransactionResult(Args&&... args)
        : op_results_(std::initializer_list<OperationResult>{std::forward<Args>(args)...})
    {}

    TransactionResult(std::vector<OperationResult>&& res)
        : op_results_(std::move(res))
    {}

    TransactionResult(const std::vector<OperationResult>& res)
        : op_results_(res)
    {}

    TransactionResult(std::initializer_list<OperationResult>&& list)
        : op_results_(std::move(list))
    {}

    TransactionResult(const TransactionResult&) = default;
    TransactionResult(TransactionResult&&) = default;

    ~TransactionResult()
    {}


    template <typename U>
    void push_back(U&& res)
    {
        op_results_.push_back(std::forward<U>(res));
    }

    template <typename... Args>
    void emplace_back(Args&&... args)
    {
        op_results_.emplace_back(std::forward<Args>(args)...);
    }

    void pop_back()
    {
        op_results_.pop_back();
    }

    auto begin() const
    {
        return op_results_.begin();
    }

    auto end() const
    {
        return op_results_.end();
    }
};
