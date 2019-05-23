#pragma once

#include <string>
#include <utility>


#include "error.hpp"



std::pair<std::string, std::string> get_protocol_address(const std::string& address)
{
    static const std::string DELIM = "://";

    const auto pos = address.find(DELIM);
    if (pos == std::string::npos)
        throw InvalidAddress("address must have format \"<protocol>://<ip>\"");

    return {address.substr(0, pos), address.substr(pos + DELIM.size())};
}


template <size_t from, typename... Ts, std::size_t... indices>
auto subtuple_impl(const std::tuple<Ts...>& tpl, std::index_sequence<indices...>)
{
    return std::make_tuple(std::get<from + indices>(tpl)...);
}


template <size_t from, size_t len, typename... Ts>
auto subtuple(const std::tuple<Ts...>& tpl)
{
    static_assert(from + len <= sizeof...(Ts));
    return subtuple_impl<from>(tpl, std::make_index_sequence<len>());
}


template <class T1, class Func>
const std::vector<std::invoke_result_t<std::decay_t<Func>, T1>> map_vector(const std::vector<T1>& vec, Func&& f)
{
    using T2 = std::invoke_result_t<std::decay_t<Func>, T1>;
    std::vector<T2> res;
    std::transform(vec.begin(), vec.end(), std::back_inserter(res), std::forward<Func>(f));
    return res;
}