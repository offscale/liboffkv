//
// Created by alloky on 20.05.19.
//

#include <iostream>

#include <gtest/gtest.h>

#include "client.hpp"
#include "key.hpp"
#include "operation.hpp"
#include "time_machine.hpp"



class UnitTestFixture : public ::testing::Test {
public:
    static
    void SetUpTestCase()
    {
        timeMachine = std::make_shared<time_machine::ThreadPool<>>();
        std::string prefix;

        std::string server_addr = SERVICE_ADDRESS;
        std::cout << "\n\n ----------------------------------------------------- \n" << std::endl;
        std::cout << "  Using server address : " << server_addr << std::endl;
        std::cout << "\n ----------------------------------------------------- \n\n" << std::endl;
        client = connect(server_addr, "/unitTests", timeMachine);
    }

    static
    void TearDownTestCase()
    {

    }

    void SetUp()
    {

    }

    void TearDown()
    {
        for (const auto& key : usedKeys) {
            try {
                client->erase(key).get();
            } catch (NoEntry& exc) {}
        }
        usedKeys.clear();
    }

    static std::shared_ptr<time_machine::ThreadPool<>> timeMachine;
    static std::unique_ptr<Client> client;
    static std::set<std::string> usedKeys;
};


std::shared_ptr<time_machine::ThreadPool<>> UnitTestFixture::timeMachine;
std::unique_ptr<Client> UnitTestFixture::client;
std::set<std::string> UnitTestFixture::usedKeys;


TEST_F(UnitTestFixture, create_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}


    ASSERT_NO_THROW(client->create("/key", "value").get());
    ASSERT_THROW(client->create("/key", "value").get(), EntryExists);

    ASSERT_THROW(client->create("/key/child/grandchild", "value").get(), NoEntry);
    ASSERT_NO_THROW(client->create("/key/child", "value").get());

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, exists_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}


    auto result = client->exists("/key").get();
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.exists);

    client->create("/key", "value").get();

    result = client->exists("/key").get();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.exists);

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, erase_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}


    ASSERT_THROW(client->erase("/key").get(), NoEntry);

    client->create("/key", "value").get();
    client->create("/key/child", "value").get();

    ASSERT_NO_THROW(client->erase("/key").get());
    ASSERT_FALSE(bool(client->exists("/key").get()));
    ASSERT_FALSE(bool(client->exists("/key/child").get()));


    uint64_t initialVersion = client->create("/key", "value").get().version;

    ASSERT_NO_THROW(client->erase("/key", initialVersion + 1u).get());
    ASSERT_TRUE(client->exists("/key").get());

    ASSERT_NO_THROW(client->erase("/key", initialVersion).get());
    ASSERT_FALSE(client->exists("/key").get());

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, exists_with_watch_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    client->create("/key", "value").get();

    std::mutex my_lock;
    my_lock.lock();

    std::thread([this, &my_lock]() mutable {
        std::lock_guard<std::mutex> lock_guard(my_lock);
        client->erase("/key").get();
    }).detach();

    auto result = client->exists("/key", true).get();
    my_lock.unlock();


    ASSERT_TRUE(result);

    result.watch.get();
    ASSERT_FALSE(client->exists("/key").get());

    usedKeys.insert("/key");
}


// TODO. Current version definitele won't work
//TEST_F(UnitTestFixture, create_with_lease_test)
//{
//    try {
//        client->erase("/key").get();
//    } catch (...) {}
//
//
//    ASSERT_NO_THROW(client->create("/key", "value", true).get());
//    ASSERT_NO_THROW(client->create("/key/child", "value", true).get());
//
//    // TODO: think about a better solution
//    auto address = client->address();
//    client.reset();
//    client = connect(address, "/unitTests", timeMachine);
//
//    ASSERT_FALSE(client->exists("/key").get());
//    ASSERT_FALSE(client->exists("/key/child").get());
//
//    usedKeys.insert("/key");
//}


TEST_F(UnitTestFixture, get_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    ASSERT_THROW(client->get("/key"), NoEntry);

    uint64_t initialVersion = client->create("/key", "value").get().version;

    GetResult result;
    ASSERT_NO_THROW({ result = client->get("/key").get(); });

    ASSERT_EQ(result.value, "value");
    ASSERT_EQ(result.version, initialVersion);

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, set_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    uint64_t initialVersion = client->create("/key", "value").get().version;
    uint64_t version = client->set("/key", "newValue").get().version;

    auto result = client->get("/key").get();

    ASSERT_GT(version, initialVersion);
    ASSERT_EQ(result.value, "newValue");
    ASSERT_EQ(result.version, version);


    ASSERT_THROW(client->set("/key/child/grandchild", "value").get(), NoEntry);
    ASSERT_NO_THROW(client->set("/key/child", "value").get());

    ASSERT_EQ(client->get("/key/child").get().value, "value");

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, get_with_watch_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    client->create("/key", "value").get();

    std::mutex my_lock;
    my_lock.lock();

    std::thread([this, &my_lock]() mutable {
        std::lock_guard<std::mutex> lock_guard(my_lock);
        client->set("/key", "newValue").get();
    }).detach();

    auto result = client->get("/key", true).get();
    my_lock.unlock();


    ASSERT_EQ(result.value, "value");

    result.watch.get();
    ASSERT_EQ(client->get("/key").get().value, "newValue");

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, cas_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    ASSERT_THROW(client->cas("/key", "value", 42u).get(), NoEntry);

    auto version = client->create("/key", "value").get().version;

    CASResult cas_result;
    GetResult get_result;


    ASSERT_NO_THROW({ cas_result = client->cas("/key", "new_value", version + 1u).get(); });

    ASSERT_FALSE(bool(cas_result));
    ASSERT_FALSE(cas_result.success);

    get_result = client->get("/key").get();
    ASSERT_EQ(get_result.version, version);
    ASSERT_EQ(get_result.value, "value");


    ASSERT_NO_THROW({ cas_result = client->cas("/key", "new_value", version).get(); });

    ASSERT_TRUE(bool(cas_result));
    ASSERT_TRUE(cas_result.success);

    get_result = client->get("/key").get();
    ASSERT_EQ(get_result.version, cas_result.version);
    ASSERT_GT(cas_result.version, version);
    ASSERT_EQ(get_result.value, "new_value");

    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, cas_zero_version_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    CASResult cas_result;
    GetResult get_result;


    ASSERT_NO_THROW({ cas_result = client->cas("/key", "value").get(); });

    ASSERT_TRUE(bool(cas_result));
    ASSERT_TRUE(cas_result.success);

    ASSERT_NO_THROW({ get_result = client->get("/key").get(); });
    ASSERT_EQ(get_result.value, "value");
    ASSERT_EQ(get_result.version, cas_result.version);

    uint64_t version = cas_result.version;


    ASSERT_NO_THROW({ cas_result = client->cas("/key", "new_value").get(); });

    ASSERT_FALSE(bool(cas_result));
    ASSERT_FALSE(cas_result.success);

    ASSERT_NO_THROW({ get_result = client->get("/key").get(); });
    ASSERT_EQ(get_result.value, "value");
    ASSERT_EQ(get_result.version, version);


    usedKeys.insert("/key");
}


TEST_F(UnitTestFixture, get_children_test)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    ASSERT_THROW(client->get_children("/key").get(), NoEntry);

    client->create("/key", "/value").get();
    client->create("/key/child", "/value").get();
    client->create("/key/child/grandchild", "/value").get();
    client->create("/key/hackerivan", "/value").get();

    ChildrenResult result;

    ASSERT_NO_THROW({ result = client->get_children("/key").get(); });

    // TODO: consider different children order
    ASSERT_EQ(result.children, std::vector<std::string>({"/key/child", "/key/hackerivan"}));


    ASSERT_NO_THROW({ result = client->get_children("/key/child").get(); });
    ASSERT_EQ(result.children, std::vector<std::string>({"/key/child", "/key/child/grandchild"}));


    usedKeys.insert("/key");
}


// TODO
TEST_F(UnitTestFixture, get_children_with_watch_test)
{

}

// TODO
TEST_F(UnitTestFixture, commit_test)
{

}