//
// Created by alloky on 20.05.19.
//

#include <iostream>

#include "gtest/gtest.h"

#include "client.hpp"
#include "key.hpp"
#include "operation.hpp"
#include "time_machine.hpp"


class UniteTestZK : public ::testing::Test
{
public:
    static void SetUpTestCase(){
        timeMachine = std::make_shared<time_machine::ThreadPool<>>();
        client = connect("zk://127.0.0.1:2181", "/uniteTestZk", timeMachine);
    }

    static void TearDownTestCase(){

    }

    void SetUp() {

    }

    void TrearDown() {

    }

    static std::shared_ptr<time_machine::ThreadPool<>> timeMachine;
    static std::unique_ptr<Client> client;
};

std::shared_ptr<time_machine::ThreadPool<>> UniteTestZK::timeMachine;
std::unique_ptr<Client> UniteTestZK::client;

TEST(sample_test_case, sample_test)
{
    EXPECT_EQ(1, 1);
}

TEST_F(UniteTestZK, set_test){
    try {
        client->erase("/key").get();
    } catch (...) {}

    ASSERT_NO_THROW(client->create("/key", "value").get());
    ASSERT_EQ(client->set("/key", "newValue").get().version, 2);
}

