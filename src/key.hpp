#pragma once

#include <string>


#include "error.hpp"


namespace detail {

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
           unit != "." && unit != ".." &&
           unit != "zookeeper";
}

} // namespace detail

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

        if (!detail::verify_unit(ans.back()))
            throw InvalidKey{};

        if (end == key.end())
            break;

        it = ++end;
    }

    return ans;
}

std::string get_parent(const std::string& key)
{
    return key.substr(0, key.size() - (std::find(key.rbegin(), key.rend(), '/') - key.rbegin() + 1));
}


class Key {
public:
    using Transformer = std::function<std::string(const std::string&, const std::vector<Key>&)>;

private:
    std::string key_;
    std::shared_ptr<std::string> prefix_;
    Transformer transformer_ = [](const std::string& prefix, const std::vector<Key>& seq) {
        return prefix + seq.rbegin()->get_raw_key();
    };

    mutable std::shared_ptr<std::vector<Key>> sequence_ = nullptr;
    mutable size_t sequence_end_ = 0;
    mutable std::string transformed_key_;

    Key(std::string key, std::shared_ptr<std::vector<Key>> sequence, const size_t end,
        std::shared_ptr<std::string> prefix)
        : key_(std::move(key)), prefix_(std::move(prefix)), sequence_(std::move(sequence)), sequence_end_(end)
    {}

    void init_sequence() const
    {
        if (sequence_)
            return;

        sequence_ = std::make_shared<std::vector<Key>>();
        for (const std::string& entry : get_entry_sequence(key_)) {
            sequence_->push_back(Key{entry, sequence_, sequence_->size() + 1, prefix_});
            sequence_->rbegin()->set_transformer(transformer_);
        }

        sequence_end_ = sequence_->size();
    }

    void perform_transformation() const
    {
        if (!transformed_key_.empty())
            return;

        if (!is_prefix_prepended())
            throw std::logic_error("Prefix is not set");

        init_sequence();
        transformed_key_ = transformer_(*prefix_, get_sequence());
    }

public:
    Key(std::string key)
        : key_(std::move(key))
    {}

    Key(const Key&) = default;

    Key& operator=(const Key&) = default;

    Key(Key&& oth) noexcept
        : key_(std::move(oth.key_)), prefix_(std::move(oth.prefix_)), transformer_(std::move(oth.transformer_)),
          sequence_(std::move(oth.sequence_)), sequence_end_(oth.sequence_end_),
          transformed_key_(std::move(oth.transformed_key_))
    {}

    Key& operator=(Key&&) = delete; // TODO if needed

    void set_prefix(const std::string& prefix)
    {
        prefix_ = std::make_shared<std::string>(prefix);
        transformed_key_ = "";
    }

    const bool is_prefix_prepended() const
    {
        return static_cast<bool>(prefix_);
    }

    const std::string& get_raw_key() const
    {
        return key_;
    }

    operator const std::string&() const
    {
        perform_transformation();
        return transformed_key_;
    }

    std::vector<Key> get_sequence() const
    {
        init_sequence();
        return std::vector<Key>(sequence_->begin(), sequence_->begin() + sequence_end_);
    }

    std::string get_parent() const
    {
        return sequence_end_ > 1 ? sequence_[sequence_end_ - 1] : "";
    }

    void set_transformer(const Transformer& transformer)
    {
        transformer_ = transformer;
        transformed_key_ = "";
    }
};
