#pragma once

#include <utility>



#include <string>


#include "operation.hpp"



struct Result {
    uint64_t version = 0;
};


struct CreateResult : Result {
};


struct SetResult : Result {
};


struct ExistsResult : Result {
    bool exists;
    std::shared_future<void> watch;

    explicit operator bool() const
    {
        return exists;
    }
};


struct GetResult : Result {
    std::string value;
    std::shared_future<void> watch;
};


struct ChildrenResult {
    std::vector<std::string> children; // TODO: substitute string with Key
    std::shared_future<void> watch;
};


struct CASResult : Result {
    bool success;

    explicit operator bool() const
    {
        return success;
    }
};


class TransactionResult {
private:
    // TODO
    struct OperationResult {
        op::op_type type;
        uint64_t version;
    };


    bool succeeded_;
    std::vector<OperationResult> op_results_;

public:
    TransactionResult()
        : succeeded_(false)
    {}

    TransactionResult(std::vector<OperationResult>&& res)
        : succeeded_(true), op_results_(std::move(res))
    {}

    TransactionResult(const std::vector<OperationResult>& res)
        : succeeded_(true), op_results_(res)
    {}

    TransactionResult(const TransactionResult&) = default;

    TransactionResult(TransactionResult&&) = default;

    TransactionResult& operator =(const TransactionResult&) = default;

    TransactionResult& operator =(TransactionResult&&) = default;

    ~TransactionResult()
    {}

    void push_back(SetResult&& res)
    {
        op_results_.push_back({op::op_type::SET, res.version});
    }

    void push_back(CreateResult&& res)
    {
        op_results_.push_back({op::op_type::CREATE, res.version});
    }

    const OperationResult& operator[](size_t index) const
    {
        return op_results_[index];
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

    bool succeeded() const
    {
        return succeeded_;
    }
};
