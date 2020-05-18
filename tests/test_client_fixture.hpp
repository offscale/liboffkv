#pragma once

#include <iostream>
#include <string>
#include <memory>
#include <utility>
#include <array>

#include <gtest/gtest.h>

#include <liboffkv/liboffkv.hpp>

extern std::string server_addr;

class ClientFixture : public ::testing::Test {
public:
    static inline std::unique_ptr <liboffkv::Client> client;

    static void SetUpTestCase()
    {
        std::cout << "\n\n ----------------------------------------------------- \n\n";
        std::cout << "  Using server address : " << server_addr << "\n";
        std::cout << "\n ----------------------------------------------------- \n\n\n";
        client = liboffkv::open(server_addr, "/unitTests");
    }

    static void TearDownTestCase()
    {}

    void SetUp()
    {}

    void TearDown()
    {}


    class KeyHolder {
    private:
        std::string key_;

        void destroy_()
        {
            if (!key_.empty())
                try {
                    client->erase(key_);
                } catch (...) {}
        }

    public:
        explicit KeyHolder(std::string key)
            : key_(std::move(key))
        {
            try {
                client->erase(key_);
            } catch (...) {}
        }

        KeyHolder(const KeyHolder&) = delete;

        KeyHolder(KeyHolder&& that)
            : key_(that.key_)
        {
            that.key_ = "";
        }

        KeyHolder& operator=(const KeyHolder&) = delete;

        KeyHolder& operator=(KeyHolder&& that)
        {
            destroy_();
            key_ = that.key_;
            that.key_ = "";
            return *this;
        }

        ~KeyHolder()
        { destroy_(); }
    };


    template <class... Keys>
    static std::array<KeyHolder, sizeof...(Keys)> hold_keys(Keys&& ... keys)
    {
        return {KeyHolder(std::move(keys))...};
    }
};
