//
// Created by alloky on 07.06.19.
//

#include <iostream>

#include <gtest/gtest.h>

#include "client.hpp"
#include "key.hpp"
#include "operation.hpp"
#include "time_machine.hpp"
#include "util.hpp"



class ClientFixture : public ::testing::Test {
public:
    static
    void SetUpTestCase()
    {
        timeMachine = std::make_shared<liboffkv::time_machine::ThreadPool<>>();
        std::string prefix;

        std::string server_addr = SERVICE_ADDRESS;
        std::cout << "\n\n ----------------------------------------------------- \n" << std::endl;
        std::cout << "  Using server address : " << server_addr << std::endl;
        std::cout << "\n ----------------------------------------------------- \n\n" << std::endl;
        client = liboffkv::connect(server_addr, "/unitTests", timeMachine);
    }

    static
    void TearDownTestCase()
    {}

    void SetUp()
    {}

    void TearDown()
    {
    }

    static std::shared_ptr<liboffkv::time_machine::ThreadPool<>> timeMachine;
    static std::unique_ptr<liboffkv::Client> client;


    class KeyHolder {
    private:
        std::string key_;

    public:
        explicit KeyHolder(std::string key)
            : key_(std::move(key))
        {
            try {
                client->erase(key_).get();
            } catch (...) {}
        }

        KeyHolder(const KeyHolder&) = delete;

        KeyHolder(KeyHolder&& oth)
            : key_(std::move(oth.key_))
        {
            oth.key_ = "";
        }

        KeyHolder& operator=(const KeyHolder&) = delete;

        KeyHolder& operator=(KeyHolder&& oth)
        {
            key_ = std::move(oth.key_);
            oth.key_ = "";
        }

        ~KeyHolder()
        {
            if (!key_.empty())
                try {
                    client->erase(key_).get();
                } catch (...) {}
        }
    };

    template <class... String>
    static std::array<KeyHolder, sizeof...(String)> holdKeys(String&& ... keys)
    {
        return {KeyHolder(std::move(keys))...};
    }
};


std::shared_ptr<liboffkv::time_machine::ThreadPool<>> ClientFixture::timeMachine;
std::unique_ptr<liboffkv::Client> ClientFixture::client;


