#pragma once

#include <exception>



class InvalidAddress : public std::exception {
public:
    virtual const char * what() const noexcept override
    {
        return "invalid address";
    }
};
