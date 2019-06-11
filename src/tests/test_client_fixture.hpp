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
        for (const auto& key : usedKeys) {
            try {
                client->erase(key).get();
            } catch (liboffkv::NoEntry& exc) {}
        }
        usedKeys.clear();
    }

    static std::shared_ptr<liboffkv::time_machine::ThreadPool<>> timeMachine;
    static std::unique_ptr<liboffkv::Client> client;
    static std::set<std::string> usedKeys;
};


std::shared_ptr<liboffkv::time_machine::ThreadPool<>> ClientFixture::timeMachine;
std::unique_ptr<liboffkv::Client> ClientFixture::client;
std::set<std::string> ClientFixture::usedKeys;


