#pragma once

#include <string>
#include <utility>


#include "error.hpp"



std::pair <std::string, std::string> get_protocol_address(const std::string& address)
{
    static const std::string DELIM = "://";

    const auto pos = address.find(DELIM);
    if (pos == std::string::npos)
        throw InvalidAddress{};

    return {address.substr(0, pos), address.substr(pos + DELIM.size())};
}
