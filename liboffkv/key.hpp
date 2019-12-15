#pragma once

#include <string>


#include "error.hpp"


namespace liboffkv { namespace key {

namespace detail {

static
bool verify_segment(const std::string &seg)
{
    for (unsigned char c : seg)
        if (c <= 0x1F || c >= 0x7F)
            return false;

    return !seg.empty() &&
           seg != "." && seg != ".." &&
           seg != "zookeeper";
}

} // namespace detail

/*
 * parses raw key
 *
 * /foo/bar/baz => std::vector<std::string>{"/foo", "/foo/bar", "/foo/bar/baz"}
 *
 * throws InvalidKey if key is incorrect
 */
std::vector<std::string> get_entry_sequence(const std::string& key)
{
    std::vector<std::string> ans;
    if (key.size() < 2)
        throw InvalidKey("key has to contain at least 2 symbols");

    if (key[0] != '/')
        throw InvalidKey("key has to begin with '/'");

    auto it = key.begin();
    while (true) {
        ++it;
        auto end = std::find(it, key.end(), '/');

        std::string segment(it, end);

        if (!detail::verify_segment(segment))
            throw InvalidKey(std::string("segment \"") + segment + "\" is invalid");

        ans.emplace_back(key.begin(), end);

        if (end == key.end())
            break;

        it = end;
    }

    return ans;
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

    bool is_prefix_prepended() const
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
        init_sequence();
        return sequence_end_ > 1 ? static_cast<std::string>((*sequence_)[sequence_end_ - 2]) : "";
    }

    void set_transformer(const Transformer& transformer)
    {
        transformer_ = transformer;
        transformed_key_ = "";
    }

    const Key with_transformer(const Transformer& transformer) const
    {
        Key res = *this;
        res.set_transformer(transformer);
        return res;
    }
};

}} // namespace key, liboffkv
