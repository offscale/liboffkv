#pragma once

#include <string>


class Result {
private:
    int64_t version_;

public:
    Result(int64_t version)
        : version_(version)
    {}

    int64_t version() const
    {
        return version_;
    }
};


class CreateResult : public Result {
public:
    CreateResult()
        : Result(0)
    {}
};


class SetResult : public Result {
public:
    SetResult(int64_t version)
        : Result(version)
    {}
};


class ExistsResult : public Result {
private:
    bool exists_;

public:
    ExistsResult(bool exists, int64_t version)
        : Result(version), exists_(exists)
    {}


    explicit operator bool() const
    {
        return exists_;
    }

    bool operator!() const
    {
        return !exists_;
    }
};


class GetResult : public Result {
private:
    std::string value_;

public:
    GetResult(std::string value, int64_t version)
            : Result(version), value_(value)
    {}


    std::string value() const
    {
        return value_;
    }
};


class CASResult : public Result {
public:
    CASResult(int64_t version)
        : Result(version)
    {}
};


class EraseResult : public Result {
public:
    EraseResult(int64_t version)
        : Result(version)
    {}
};