#pragma once

#include <string>


#include "error.hpp"



bool verify_unit(const std::string& unit)
{
    return unit.size() > 0 &&
           unit.find("...") == std::string::npos &&
           unit != "." && unit != "..";
}


std::vector <std::string> parse(const std::string& key)
{
    std::vector <std::string> ans;
    if (key.size() < 2 || key[0] != '/')
        throw InvalidKey{};

    auto it = ++key.begin();
    while (it != key.end()) {
        auto end = std::find(it, key.end(), '/');
        ans.push_back(std::string(it, end));

        if (!verify_unit(ans.back()))
            throw InvalidKey{};

        if (end == key.end())
            break;

        it = ++end;
    }

    return ans;
}