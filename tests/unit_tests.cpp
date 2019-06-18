//
// Created by alloky on 20.05.19.
//

#include "test_client_fixture.hpp"
#include <chrono>


TEST_F(ClientFixture, key_validation_test)
{
    const auto check_key = [](const std::string &path) {
        liboffkv::key::Key key(path);
        key.set_prefix("");
        static_cast<std::string>(key);
    };

    ASSERT_THROW(check_key(""),                  liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/"),                 liboffkv::InvalidKey);
    ASSERT_THROW(check_key("mykey"),             liboffkv::InvalidKey);

    ASSERT_NO_THROW(check_key("/mykey"));
    ASSERT_NO_THROW(check_key("/mykey/child"));

    ASSERT_NO_THROW(check_key("/каша"));
    ASSERT_NO_THROW(check_key("/κόσμε"));
    ASSERT_NO_THROW(check_key("/�"));  // "/\xEF\xBF\xBD"
    ASSERT_NO_THROW(check_key("/他")); // "/\xE4\xBB\x96"
    ASSERT_NO_THROW(check_key("/𠜎")); // "/\xF0\xA0\x9C\x8E"

    ASSERT_THROW(check_key("/\xC0"),     liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/\xC1"),     liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/\xFE"),     liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/test\xFF"), liboffkv::InvalidKey);

    ASSERT_THROW(check_key(std::string("/\0\xFF",       3)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/\0/",          3)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/\0/zookeeper", 12)), liboffkv::InvalidKey);

    ASSERT_THROW(check_key("/zookeeper"),        liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/zookeeper/child"),  liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/zookeeper/.."),     liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/two//three"),   liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/two/three/"),   liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/two/three/."),  liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/two/three/.."), liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/./three"),      liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/../three"),     liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/one/zookeeper"),    liboffkv::InvalidKey);
}


TEST_F(ClientFixture, create_test)
{
    auto holder = holdKeys("/key");

    ASSERT_NO_THROW(client->create("/key", "value").get());
    ASSERT_THROW(client->create("/key", "value").get(), liboffkv::EntryExists);

    ASSERT_THROW(client->create("/key/child/grandchild", "value").get(), liboffkv::NoEntry);
    ASSERT_NO_THROW(client->create("/key/child", "value").get());
}


TEST_F(ClientFixture, exists_test)
{
    auto holder = holdKeys("/key");

    auto result = client->exists("/key").get();
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.exists);

    client->create("/key", "value").get();

    result = client->exists("/key").get();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.exists);
}


TEST_F(ClientFixture, erase_test)
{
    auto holder = holdKeys("/key");

    ASSERT_THROW(client->erase("/key").get(), liboffkv::NoEntry);

    client->create("/key", "value").get();
    client->create("/key/child", "value").get();

    ASSERT_NO_THROW(client->erase("/key").get());
    ASSERT_FALSE(client->exists("/key").get());
    ASSERT_FALSE(client->exists("/key/child").get());


    uint64_t initialVersion = client->create("/key", "value").get().version;

    ASSERT_NO_THROW(client->erase("/key", initialVersion + 1u).get());
    ASSERT_TRUE(client->exists("/key").get());

    ASSERT_NO_THROW(client->erase("/key", initialVersion).get());
    ASSERT_FALSE(client->exists("/key").get());
}


TEST_F(ClientFixture, exists_with_watch_test)
{
    auto holder = holdKeys("/key");

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
}


TEST_F(ClientFixture, create_with_lease_test)
{
    auto holder = holdKeys("/key");
    using namespace std::chrono_literals;

    {
        auto local_client = liboffkv::connect(SERVICE_ADDRESS, "/unitTests", timeMachine);
        ASSERT_NO_THROW(local_client->create("/key", "value", true).get());

        std::this_thread::sleep_for(25s);

        ASSERT_TRUE(client->exists("/key").get());
    }

    std::this_thread::sleep_for(25s);

    {
        auto local_client = liboffkv::connect(SERVICE_ADDRESS, "/unitTests", timeMachine);
        ASSERT_FALSE(client->exists("/key").get());
    }
}


TEST_F(ClientFixture, get_test)
{
    auto holder = holdKeys("/key");

    ASSERT_THROW(client->get("/key").get(), liboffkv::NoEntry);

    uint64_t initialVersion = client->create("/key", "value").get().version;

    liboffkv::GetResult result;
    ASSERT_NO_THROW({ result = client->get("/key").get(); });

    ASSERT_EQ(result.value, "value");
    ASSERT_EQ(result.version, initialVersion);
}


TEST_F(ClientFixture, set_test)
{
    auto holder = holdKeys("/key");

    uint64_t initialVersion = client->create("/key", "value").get().version;
    uint64_t version = client->set("/key", "newValue").get().version;

    auto result = client->get("/key").get();

    ASSERT_GT(version, initialVersion);
    ASSERT_EQ(result.value, "newValue");
    ASSERT_EQ(result.version, version);


    ASSERT_THROW(client->set("/key/child/grandchild", "value").get(), liboffkv::NoEntry);
    ASSERT_NO_THROW(client->set("/key/child", "value").get());

    ASSERT_EQ(client->get("/key/child").get().value, "value");
}


TEST_F(ClientFixture, get_with_watch_test)
{
    auto holder = holdKeys("/key");

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
}


TEST_F(ClientFixture, cas_test)
{
    auto holder = holdKeys("/key");

    ASSERT_THROW(client->cas("/key", "value", 42u).get(), liboffkv::NoEntry);

    auto version = client->create("/key", "value").get().version;

    liboffkv::CASResult cas_result;
    liboffkv::GetResult get_result;

    ASSERT_NO_THROW({ cas_result = client->cas("/key", "new_value", version + 1u).get(); });


    ASSERT_FALSE(cas_result);
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
}


TEST_F(ClientFixture, cas_zero_version_test)
{
    auto holder = holdKeys("/key");

    liboffkv::CASResult cas_result;
    liboffkv::GetResult get_result;


    ASSERT_NO_THROW({ cas_result = client->cas("/key", "value").get(); });

    ASSERT_TRUE(cas_result);
    ASSERT_TRUE(cas_result.success);

    ASSERT_NO_THROW({ get_result = client->get("/key").get(); });
    ASSERT_EQ(get_result.value, "value");
    ASSERT_EQ(get_result.version, cas_result.version);
    uint64_t version = cas_result.version;


    ASSERT_NO_THROW({ cas_result = client->cas("/key", "new_value").get(); });

    ASSERT_FALSE(cas_result);
    ASSERT_FALSE(cas_result.success);

    ASSERT_NO_THROW({ get_result = client->get("/key").get(); });
    ASSERT_EQ(get_result.value, "value");
    ASSERT_EQ(get_result.version, version);
}


TEST_F(ClientFixture, get_children_test)
{
    auto holder = holdKeys("/key");

    ASSERT_THROW(client->get_children("/key").get(), liboffkv::NoEntry);

    client->create("/key", "/value").get();
    client->create("/key/child", "/value").get();
    client->create("/key/child/grandchild", "/value").get();
    client->create("/key/hackerivan", "/value").get();

    liboffkv::ChildrenResult result;

    ASSERT_NO_THROW({ result = client->get_children("/key").get(); });

    ASSERT_TRUE(liboffkv::util::equal_as_sets(result.children, std::vector<std::string>({"/key/child", "/key/hackerivan"})));


    ASSERT_NO_THROW({ result = client->get_children("/key/child").get(); });
    ASSERT_TRUE(liboffkv::util::equal_as_sets(result.children, std::vector<std::string>({"/key/child/grandchild"})));
}


TEST_F(ClientFixture, get_children_with_watch_test)
{
    auto holder = holdKeys("/key");

    client->create("/key", "value").get();
    client->create("/key/child", "value").get();
    client->create("/key/child/grandchild", "value").get();
    client->create("/key/dimak24", "value").get();

    std::mutex my_lock;
    my_lock.lock();

    std::thread([this, &my_lock]() mutable {
        std::lock_guard<std::mutex> lock_guard(my_lock);
        client->erase("/key/dimak24").get();
    }).detach();

    auto result = client->get_children("/key", true).get();
    my_lock.unlock();


    ASSERT_TRUE(liboffkv::util::equal_as_sets(result.children, std::vector<std::string>({"/key/child", "/key/dimak24"})));

    result.watch.get();
    ASSERT_TRUE(liboffkv::util::equal_as_sets(client->get_children("/key").get().children,
                                              std::vector<std::string>({"/key/child"})));
}


TEST_F(ClientFixture, commit_test)
{
    auto holder = holdKeys("/key", "/foo");

    auto key_version = client->create("/key", "value").get().version;
    auto foo_version = client->create("/foo", "value").get().version;
    auto bar_version = client->create("/foo/bar", "value").get().version;

    // check fails
    try {
        client->commit(
            {
                {
                    liboffkv::op::Check("/key", key_version),
                    liboffkv::op::Check("/foo", foo_version + 1u),
                    liboffkv::op::Check("/foo/bar", bar_version),
                },
                {
                    liboffkv::op::create("/key/child", "value"),
                    liboffkv::op::set("/key", "new_value"),
                    liboffkv::op::erase("/foo"),
                }
            }
        ).get();

        FAIL() << "Expected commit to throw TransactionFailed, but it threw nothing";
    } catch (liboffkv::TransactionFailed& e) {
        ASSERT_EQ(e.failed_operation_index(), 1);
    } catch (std::exception& e) {
        FAIL() << "Expected commit to throw TransactionFailed, but it threw different exception:\n" << e.what();
    }

    ASSERT_FALSE(client->exists("/key/child").get());
    ASSERT_EQ(client->get("/key").get().value, "value");
    ASSERT_TRUE(client->exists("/foo").get());


    // op fails
    try {
        client->commit(
            {
                {
                    liboffkv::op::Check("/key", key_version),
                    liboffkv::op::Check("/foo", foo_version),
                    liboffkv::op::Check("/foo/bar", bar_version),
                },
                {
                    liboffkv::op::create("/key/child", "value"),
                    liboffkv::op::create("/key/hackerivan", "new_value"),
                    liboffkv::op::erase("/foo"),
                    // this fails because /key/child/grandchild does not exist
                    liboffkv::op::set("/key/child/grandchild", "new_value"),
                    liboffkv::op::erase("/asfdsfasdfa"),
                }
            }
        ).get();

        FAIL() << "Expected commit to throw TransactionFailed, but it threw nothing";
    } catch (liboffkv::TransactionFailed& e) {
        ASSERT_EQ(e.failed_operation_index(), 6);
    } catch (std::exception& e) {
        FAIL() << "Expected commit to throw TransactionFailed, but it threw different exception:\n" << e.what();
    }

    ASSERT_FALSE(client->exists("/key/child").get());
    ASSERT_TRUE(client->exists("/foo").get());


    // everything is OK
    liboffkv::TransactionResult result;

    ASSERT_NO_THROW({
        result = client->commit(
            {
                {
                    liboffkv::op::Check("/key", key_version),
                    liboffkv::op::Check("/foo", foo_version),
                    liboffkv::op::Check("/foo/bar", bar_version),
                },
                {
                    liboffkv::op::create("/key/child", "value"),
                    liboffkv::op::set("/key", "new_value"),
                    liboffkv::op::erase("/foo"),
                }
            }
        ).get();
    });

    ASSERT_TRUE(client->exists("/key/child").get());
    ASSERT_EQ(client->get("/key").get().value, "new_value");
    ASSERT_FALSE(client->exists("/foo").get());

    ASSERT_GT(result[1].version, key_version);
}

TEST_F(ClientFixture, erase_prefix_test)
{
    auto holder = holdKeys("/ichi", "/ichinichi");

    ASSERT_NO_THROW(client->create("/ichi",      "one").get());
    ASSERT_NO_THROW(client->create("/ichinichi", "two").get());

    ASSERT_NO_THROW(client->erase("/ichi").get());

    ASSERT_TRUE(static_cast<bool>(client->exists("/ichinichi").get()));
}

TEST_F(ClientFixture, get_prefix_test)
{
    auto holder = holdKeys("/sore", "/sorewanan");

    ASSERT_NO_THROW(client->create("/sore",      "1").get());
    ASSERT_NO_THROW(client->create("/sore/ga",   "2").get());
    ASSERT_NO_THROW(client->create("/sorewanan", "3").get());

    liboffkv::ChildrenResult res;
    ASSERT_NO_THROW(res = client->get_children("/sore").get());

    ASSERT_EQ(res.children.size(), 1);
    ASSERT_EQ(res.children[0], "/sore/ga");
}
