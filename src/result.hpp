#pragma once

#include <string>


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
