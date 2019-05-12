#pragma once

#include <string>
#include <utility>


#include "error.hpp"



std::pair<std::string, std::string> get_protocol_address(const std::string& address)
{
    static const std::string DELIM = "://";

    const auto pos = address.find(DELIM);
    if (pos == std::string::npos)
        throw InvalidAddress{};

    return {address.substr(0, pos), address.substr(pos + DELIM.size())};
}


template <size_t from, typename... Ts, std::size_t... indices>
auto subtuple_impl(const std::tuple<T...>& tpl, std::index_sequence<indices...>)
{
    return std::make_tuple(std::get<from + indices>(tpl)...);
}

template <size_t from, size_t len, typename... Ts>
auto subtuple(const std::tuple<Ts...>& tpl)
{
    static_assert(from + len <= sizeof...(Ts));
    return subtuple_impl<from>(tpl, std::make_index_sequence<len>());
}