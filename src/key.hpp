#pragma once

#include <string>


#include "error.hpp"



bool is_ascii(unsigned char ch)
{
    return ch < 128;
}

unsigned bytes_number(unsigned char ch)
{
    switch (ch >> 3) {
        case 0b11110:
            return 4u;
        case 0b1110:
            return 3u;
        case 0b110:
            return 2u;
        default:
            return 0u;
    }
}

bool is_multibyte_symbol_part(unsigned char ch)
{
    return (ch >> 6) == 0b10;
}

bool unicode_allowed(unsigned code)
{
    return code != 0u &&
           !(0x0001 <= code && code <= 0x001F) &&
           !(0x007F <= code && code <= 0x009F) &&
           !(0xD800 <= code && code <= 0xF8FF) &&
           !(0xFFF0 <= code && code <= 0xFFFF);
}

bool verify_unit(const std::string& unit)
{
    auto ptr = reinterpret_cast<const unsigned char*>(unit.c_str());
    while (*ptr) {
        if (is_ascii(*ptr))
            ++ptr;
        else {
            auto bytes = bytes_number(*ptr);
            unsigned unicode_number = (*ptr) % (1 << static_cast<unsigned>(7 - bytes));

            for (unsigned i = 1; i < bytes; ++i) {
                if (!ptr[i] || !is_multibyte_symbol_part(ptr[i]))
                    return false;
                unicode_number = (unicode_number << 6) + (ptr[i] % (1 << 6));
            }

            if (!unicode_allowed(unicode_number))
                return false;

            ptr += bytes;
        }
    }

    return unit.size() > 0 &&
           unit.find("...") == std::string::npos &&
           unit != "." && unit != ".." &&
           unit != "zookeeper";
}


/*
 * /foo/bar/baz => std::vector<std::string>{"/foo", "/foo/bar", "/foo/bar/baz"}
 *
 * throws InvalidKey if key is incorrect
 */
std::vector<std::string> get_entry_sequence(const std::string& key)
{
    std::vector<std::string> ans;
    if (key.size() < 2 || key[0] != '/')
        throw InvalidKey{};

    auto it = ++key.begin();
    while (it != key.end()) {
        auto end = std::find(it, key.end(), '/');
        ans.push_back((ans.size() ? ans.back() : std::string()) + "/" + std::string(it, end));

        if (!verify_unit(ans.back()))
            throw InvalidKey{};

        if (end == key.end())
            break;

        it = ++end;
    }

    return ans;
}