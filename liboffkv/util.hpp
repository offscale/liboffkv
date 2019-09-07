#pragma once

#include <string>
#include <utility>
#include <vector>
#include <set>
#include <type_traits>

namespace liboffkv::util {

template<class T>
struct always_false : std::false_type {};

std::pair<std::string, std::string> split_url(const std::string &url)
{
    static const std::string DELIM = "://";

    const auto pos = url.find(DELIM);

    if (pos == std::string::npos)
        throw InvalidAddress("URL must be of 'protocol://address' format");

    return {url.substr(0, pos), url.substr(pos + DELIM.size())};
}

template<class T>
bool equal_as_unordered(const std::vector<T> &a, const std::vector<T> &b)
{
    return std::multiset<T>(a.begin(), a.end()) == std::multiset<T>(b.begin(), b.end());
}

} // namespace liboffkv::util
