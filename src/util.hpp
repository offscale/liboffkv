#pragma once

#include <string>
#include <utility>


#include "error.hpp"


namespace liboffkv { namespace util {

namespace detail {

template <size_t from, typename... Ts, std::size_t... indices>
auto subtuple_impl(const std::tuple<Ts...>& tpl, std::index_sequence<indices...>)
{
    return std::make_tuple(std::get<from + indices>(tpl)...);
}

} // namespace detail


std::pair<std::string, std::string> get_protocol_address(const std::string& address)
{
    static const std::string DELIM = "://";

    const auto pos = address.find(DELIM);
    if (pos == std::string::npos)
        throw InvalidAddress("address must have format \"<protocol>://<ip>\"");

    return {address.substr(0, pos), address.substr(pos + DELIM.size())};
}


template <size_t from, size_t len, typename... Ts>
auto subtuple(const std::tuple<Ts...>& tpl)
{
    static_assert(from + len <= sizeof...(Ts));
    return detail::subtuple_impl<from>(tpl, std::make_index_sequence<len>());
}


template <class T1, class Func>
const std::vector<std::invoke_result_t<std::decay_t<Func>, T1>> map_vector(const std::vector<T1>& vec, Func&& f)
{
    using T2 = std::invoke_result_t<std::decay_t<Func>, T1>;
    std::vector<T2> res;
    std::transform(vec.begin(), vec.end(), std::back_inserter(res), std::forward<Func>(f));
    return res;
}


template <typename T, typename F>
const std::vector<T> filter_vector(const std::vector<T>& v, F&& f)
{
    auto copy = v;
    copy.erase(std::remove_if(copy.begin(), copy.end(),
               [f = std::forward<F>(f)](const T& x) {return !f(x);}), copy.end());
    return copy;
}


template <typename T>
bool equal_as_sets(const std::vector<T>& v1, const std::vector<T>& v2)
{
    if (v1.size() != v2.size())
        return false;

    std::multiset<T> m1(v1.begin(), v1.end());
    std::multiset<T> m2(v2.begin(), v2.end());

    return m1 == m2;
}


template <class T>
T call_get(std::future<T>&& future)
{
    try {
        return future.get();
    } liboffkv_catch
}

template <class T>
void call_get_ignore(std::future<T>&& future)
{
    try {
        future.get();
    } liboffkv_catch
}

template <class T>
void call_get_ignore_noexcept(std::future<T>&& future)
{
    try {
        future.get();
    } catch (...) {}
}

}} // namespace util, liboffkv