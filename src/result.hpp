#pragma once

#include <utility>
#include <string>


#include "operation.hpp"


namespace liboffkv {

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

namespace util {

// We assume that each user operation maps to zero or more auxiliary (ResultKind::AUX or ResultKind::AUX_NO_RESULT)
// operations, followed by exactly one non-auxiliary one.
enum class ResultKind {
    // This operation corresponds to a check user operation, produces 1 result.
        CHECK,

    // This operation corresponds to an op::op_type::CREATE user operation, produces 1 result.
        CREATE,

    // This operation corresponds to an op::op_type::SET user operation, produces 1 result.
        SET,

    // This operation corresponds to an op::op_type::ERASE user operation, produces no results.
        ERASE_NO_RESULT,

    // This is an auxiliary operation, produces 1 result.
        AUX,

    // This is an auxiliary operation, produces no results.
        AUX_NO_RESULT,
};

size_t compute_offkv_operation_index(const std::vector<ResultKind>& result_kinds, const size_t raw_operation_index) {
    size_t ncompleted = 0;

    for (size_t i = 0; i < raw_operation_index; ++i) {
        switch (result_kinds[i]) {
            case ResultKind::CHECK:
            case ResultKind::CREATE:
            case ResultKind::SET:
            case ResultKind::ERASE_NO_RESULT:
                ++ncompleted;
                break;
            case ResultKind::AUX:
            case ResultKind::AUX_NO_RESULT:
                break;
        }
    }

    return ncompleted;
}

} // namespace util
} // namespace liboffkv