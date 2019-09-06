#pragma once

#include <string>
#include <string_view>
#include <algorithm>
#include <vector>
#include "errors.hpp"

namespace liboffkv {

class Path
{
private:
    static bool validate_segment_(std::string_view segment)
    {
        for (unsigned char c : segment)
            if (c <= 0x1F || c >= 0x7F)
                return false;
        return !segment.empty() &&
               segment != "." &&
               segment != ".." &&
               segment != "zookeeper";
    }

protected:
    std::string path_;

public:
    std::vector<std::string_view> segments()
    {
        if (path_.empty() || path_[0] != '/')
            throw InvalidKey{path_};
        if (path_.size() == 1)
            return {};
        std::vector<std::string_view> result;
        auto it = path_.data(), end = it + path_.size();
        while (true) {
            ++it;
            auto segment_end = std::find(it, end, '/');
            result.emplace_back(it, segment_end - it);
            if (segment_end == end)
                break;
            it = segment_end;
        }
        return result;
    }

    template<class T>
    Path(T &&path)
        : path_(std::forward<T>(path))
    {
        for (auto segment : segments())
            if (!validate_segment_(segment))
                throw InvalidKey{path_};
    }

    Path parent() const { return Path{path_.substr(0, path_.rfind('/'))}; }

    bool root() const { return path_.size() == 1; }

    Path operator /(const Path &that) const
    {
        if (root())
            return that;
        if (that.root())
            return *this;
        return Path{path_ + that.path_};
    }
};

class Key : public Path
{
public:
    template<class T>
    Key(T &&key)
        : Path(std::forward<T>(key))
    {
        if (root())
            throw InvalidKey{path_};
    }
};

} // namespace liboffkv
