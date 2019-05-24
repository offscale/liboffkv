//
// Created by alloky on 20.05.19.
//

#include <iostream>

#include "gtest/gtest.h"

#include "client.hpp"
#include "key.hpp"
#include "operation.hpp"
#include "time_machine.hpp"

class UniteTestFixture : public ::testing::Test
{
public:
    static void SetUpTestCase(){
        timeMachine = std::make_shared<time_machine::ThreadPool<>>();
        std::string prefix;

        std::string server_addr = SERVICE_ADDRESS;
        std::cout << "\n\n ----------------------------------------------------- \n" << std::endl;
        std::cout << "  Using server address : " << server_addr << std::endl;
        std::cout << "\n ----------------------------------------------------- \n\n" << std::endl;
        client = connect(server_addr, "/uniteTests", timeMachine);
    }

    static void TearDownTestCase(){

    }

    void SetUp() {

    }

    void TrearDown() {
        for(auto& key : usedKeys){
            try {
                client->erase(key).get();
            } catch (NoEntry& exc){}
        }
        usedKeys.clear();
    }

    static std::shared_ptr<time_machine::ThreadPool<>> timeMachine;
    static std::unique_ptr<Client> client;
    static std::vector<std::string> usedKeys;
};

std::shared_ptr<time_machine::ThreadPool<>> UniteTestFixture::timeMachine;
std::unique_ptr<Client> UniteTestFixture::client;
std::vector<std::string> UniteTestFixture::usedKeys;


TEST_F(UniteTestFixture, create_test) {
    try {
        client->erase("/key").get();
    } catch (...) {}

    ASSERT_NO_THROW(client->create("/key", "value").get());
    usedKeys.push_back("/key");
}


TEST_F(UniteTestFixture, erase_test){
    ASSERT_THROW(client->erase("/key").get(), NoEntry);
    usedKeys.push_back("/key");
}

TEST_F(UniteTestFixture, set_test){
    try {
        client->erase("/key").get();
    } catch (...) {}

    ASSERT_NO_THROW(client->create("/key", "value").get());
    ASSERT_EQ(client->set("/key", "newValue").get().version, 2);
    usedKeys.push_back("/key");
}



