#include "test_client_fixture.hpp"
#include <liboffkv/liboffkv.hpp>
#include <chrono>
#include <mutex>
#include <thread>
#include <iostream>


TEST_F(ClientFixture, key_validation_test)
{
    const auto check_key = [](const std::string &path) {
        liboffkv::Key{path};
    };

    ASSERT_THROW(check_key(""),                  liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/"),                 liboffkv::InvalidKey);
    ASSERT_THROW(check_key("mykey"),             liboffkv::InvalidKey);

    ASSERT_NO_THROW(check_key("/mykey"));
    ASSERT_NO_THROW(check_key("/mykey/child"));

    ASSERT_THROW(check_key("/каша"),     liboffkv::InvalidKey);
    ASSERT_THROW(check_key("/test\xFF"), liboffkv::InvalidKey);

    ASSERT_THROW(check_key(std::string("/test\0",   6)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/test\x01", 6)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/test\t",   6)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/test\n",   6)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/test\x1F", 6)), liboffkv::InvalidKey);
    ASSERT_THROW(check_key(std::string("/test\x7F", 6)), liboffkv::InvalidKey);

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

    ASSERT_NO_THROW(check_key("/.../.../zookeper"));
}

TEST_F(ClientFixture, create_large_test)
{
    auto holder = hold_keys("/key");

    std::string value(65'000, '\1');

    ASSERT_NO_THROW(client->create("/key", value));
    ASSERT_THROW(client->create("/key", value), liboffkv::EntryExists);

    ASSERT_THROW(client->create("/key/child/grandchild", value), liboffkv::NoEntry);
    ASSERT_NO_THROW(client->create("/key/child", value));
}

TEST_F(ClientFixture, create_test)
{
    auto holder = hold_keys("/key");

    ASSERT_NO_THROW(client->create("/key", "value"));
    ASSERT_THROW(client->create("/key", "value"), liboffkv::EntryExists);

    ASSERT_THROW(client->create("/key/child/grandchild", "value"), liboffkv::NoEntry);
    ASSERT_NO_THROW(client->create("/key/child", "value"));
}

TEST_F(ClientFixture, exists_test)
{
    auto holder = hold_keys("/key");

    auto result = client->exists("/key");
    ASSERT_FALSE(result);

    client->create("/key", "value");

    result = client->exists("/key");
    ASSERT_TRUE(result);
}


TEST_F(ClientFixture, erase_test)
{
    auto holder = hold_keys("/key");

    ASSERT_THROW(client->erase("/key"), liboffkv::NoEntry);

    client->create("/key", "value");
    client->create("/key/child", "value");

    ASSERT_NO_THROW(client->erase("/key"));
    ASSERT_FALSE(client->exists("/key"));
    ASSERT_FALSE(client->exists("/key/child"));
}


TEST_F(ClientFixture, versioned_erase_test)
{
    auto holder = hold_keys("/key");

    int64_t version = client->create("/key", "value");
    client->create("/key/child", "value");

    ASSERT_NO_THROW(client->erase("/key", version + 1));
    ASSERT_TRUE(client->exists("/key"));
    ASSERT_TRUE(client->exists("/key/child"));

    ASSERT_NO_THROW(client->erase("/key", version));
    ASSERT_FALSE(client->exists("/key"));
    ASSERT_FALSE(client->exists("/key/child"));
}


TEST_F(ClientFixture, exists_with_watch_test)
{
    auto holder = hold_keys("/key");

    client->create("/key", "value");

    std::mutex my_lock;
    my_lock.lock();

    auto thread = std::thread([&my_lock]() mutable {
        std::lock_guard<std::mutex> lock_guard(my_lock);
        client->erase("/key");
    });

    auto result = client->exists("/key", true);
    my_lock.unlock();

    ASSERT_TRUE(result);

    thread.join();

    result.watch->wait();
    ASSERT_FALSE(client->exists("/key"));
}


TEST_F(ClientFixture, create_with_lease_test)
{
    auto holder = hold_keys("/key");
    using namespace std::chrono_literals;

    {
        auto local_client = liboffkv::open(server_addr, "/unitTests");
        ASSERT_NO_THROW(local_client->create("/key", "value", true));

        std::this_thread::sleep_for(25s);

        ASSERT_TRUE(client->exists("/key"));
    }

    std::this_thread::sleep_for(25s);

    {
        auto local_client = liboffkv::open(server_addr, "/unitTests");
        ASSERT_FALSE(client->exists("/key"));
    }
}


TEST_F(ClientFixture, get_test)
{
    auto holder = hold_keys("/key");

    ASSERT_THROW(client->get("/key"), liboffkv::NoEntry);

    int64_t initialVersion = client->create("/key", "value");

    liboffkv::GetResult result;
    ASSERT_NO_THROW(result = client->get("/key"));

    ASSERT_EQ(result.value, "value");
    ASSERT_EQ(result.version, initialVersion);
}


TEST_F(ClientFixture, set_test)
{
    auto holder = hold_keys("/key");

    int64_t initialVersion = client->create("/key", "value");
    int64_t version = client->set("/key", "newValue");

    client->set("/another", "anything");

    auto result = client->get("/key");

    ASSERT_GT(version, initialVersion);
    ASSERT_EQ(result.value, "newValue");
    ASSERT_EQ(result.version, version);

    ASSERT_THROW(client->set("/key/child/grandchild", "value"), liboffkv::NoEntry);
    ASSERT_NO_THROW(client->set("/key/child", "value"));

    ASSERT_EQ(client->get("/key/child").value, "value");
}


TEST_F(ClientFixture, get_with_watch_test)
{
    auto holder = hold_keys("/key");

    client->create("/key", "value");

    std::mutex my_lock;
    my_lock.lock();

    auto thread = std::thread([&my_lock]() mutable {
        std::lock_guard<std::mutex> lock_guard(my_lock);
        client->set("/key", "newValue");
    });

    auto result = client->get("/key", true);
    my_lock.unlock();

    ASSERT_EQ(result.value, "value");

    result.watch->wait();

    thread.join();

    ASSERT_EQ(client->get("/key").value, "newValue");
}


TEST_F(ClientFixture, cas_test)
{
    auto holder = hold_keys("/key");

    ASSERT_THROW(client->cas("/key", "value", 42), liboffkv::NoEntry);

    auto version = client->create("/key", "value");

    liboffkv::CasResult cas_result;
    liboffkv::GetResult get_result;

    ASSERT_NO_THROW(cas_result = client->cas("/key", "new_value", version + 1));

    ASSERT_FALSE(cas_result);

    get_result = client->get("/key");
    ASSERT_EQ(get_result.version, version);
    ASSERT_EQ(get_result.value, "value");

    ASSERT_NO_THROW(cas_result = client->cas("/key", "new_value", version));

    ASSERT_TRUE(cas_result);

    get_result = client->get("/key");
    ASSERT_EQ(get_result.version, cas_result.version);
    ASSERT_GT(cas_result.version, version);
    ASSERT_EQ(get_result.value, "new_value");
}


TEST_F(ClientFixture, cas_zero_version_test)
{
    auto holder = hold_keys("/key");

    liboffkv::CasResult cas_result;
    liboffkv::GetResult get_result;

    ASSERT_NO_THROW(cas_result = client->cas("/key", "value"));

    ASSERT_TRUE(cas_result);

    ASSERT_NO_THROW(get_result = client->get("/key"));
    ASSERT_EQ(get_result.value, "value");
    ASSERT_EQ(get_result.version, cas_result.version);
    int64_t version = cas_result.version;

    ASSERT_NO_THROW(cas_result = client->cas("/key", "new_value"));

    ASSERT_FALSE(cas_result);

    ASSERT_NO_THROW(get_result = client->get("/key"));
    ASSERT_EQ(get_result.value, "value");
    ASSERT_EQ(get_result.version, version);
}


TEST_F(ClientFixture, get_children_test)
{
    auto holder = hold_keys("/key");

    ASSERT_THROW(client->get_children("/key"), liboffkv::NoEntry);

    client->create("/key", "/value");
    client->create("/key/child", "/value");
    client->create("/key/child/grandchild", "/value");
    client->create("/key/hackerivan", "/value");

    liboffkv::ChildrenResult result;

    ASSERT_NO_THROW(result = client->get_children("/key"));

    ASSERT_TRUE(liboffkv::detail::equal_as_unordered(
        result.children,
        {"/key/child", "/key/hackerivan"}
    ));

    ASSERT_NO_THROW(result = client->get_children("/key/child"));
    ASSERT_TRUE(liboffkv::detail::equal_as_unordered(
        result.children,
        {"/key/child/grandchild"}
    ));
}


TEST_F(ClientFixture, get_children_with_watch_test)
{
    auto holder = hold_keys("/key");

    client->create("/key", "value");
    client->create("/key/child", "value");
    client->create("/key/child/grandchild", "value");
    client->create("/key/dimak24", "value");

    std::mutex my_lock;
    my_lock.lock();

    auto thread = std::thread([&my_lock]() mutable {
        std::lock_guard<std::mutex> lock_guard(my_lock);
        client->erase("/key/dimak24");
    });

    auto result = client->get_children("/key", true);
    my_lock.unlock();

    ASSERT_TRUE(liboffkv::detail::equal_as_unordered(
        result.children,
        {"/key/child", "/key/dimak24"}
    ));

    result.watch->wait();

    thread.join();

    ASSERT_TRUE(liboffkv::detail::equal_as_unordered(
        client->get_children("/key").children,
        {"/key/child"}
    ));
}


TEST_F(ClientFixture, commit_test)
{
    auto holder = hold_keys("/key", "/foo");

    auto key_version = client->create("/key", "value");
    auto foo_version = client->create("/foo", "value");
    auto bar_version = client->create("/foo/bar", "value");
    client->create("/foo/bar/subbar", "value");

    // check fails
    try {
        client->commit(
            {
                {
                    liboffkv::TxnCheck("/key", key_version),
                    liboffkv::TxnCheck("/foo", foo_version + 1),
                    liboffkv::TxnCheck("/foo/bar", bar_version),
                },
                {
                    liboffkv::TxnOpCreate("/key/child", "value"),
                    liboffkv::TxnOpSet("/key", "new_value"),
                    liboffkv::TxnOpErase("/foo"),
                }
            }
        );
        FAIL() << "Expected commit to throw TxnFailed, but it threw nothing";
    } catch (liboffkv::TxnFailed &e) {
        ASSERT_EQ(e.failed_op(), 1);
    } catch (std::exception &e) {
        FAIL() << "Expected commit to throw TxnFailed, "
                  "but it threw different exception:\n" << e.what();
    }

    ASSERT_FALSE(client->exists("/key/child"));
    ASSERT_EQ(client->get("/key").value, "value");
    ASSERT_TRUE(client->exists("/foo"));

    // op fails
    try {
        client->commit(
            {
                {
                    liboffkv::TxnCheck("/key", key_version),
                    liboffkv::TxnCheck("/foo", foo_version),
                    liboffkv::TxnCheck("/foo/bar", bar_version),
                },
                {
                    liboffkv::TxnOpCreate("/key/child", "value"),
                    liboffkv::TxnOpCreate("/key/hackerivan", "new_value"),
                    liboffkv::TxnOpErase("/foo"),
                    // this fails because /key/child/grandchild does not exist
                    liboffkv::TxnOpSet("/key/child/grandchild", "new_value"),
                    liboffkv::TxnOpErase("/asfdsfasdfa"),
                }
            }
        );
        FAIL() << "Expected commit to throw TxnFailed, but it threw nothing";
    } catch (liboffkv::TxnFailed &e) {
        ASSERT_EQ(e.failed_op(), 6);
    } catch (std::exception &e) {
        FAIL() << "Expected commit to throw TxnFailed, but it threw different exception:\n" << e.what();
    }

    ASSERT_FALSE(client->exists("/key/child"));
    ASSERT_TRUE(client->exists("/foo"));

    // everything is OK
    liboffkv::TransactionResult result;

    ASSERT_NO_THROW(result = client->commit(
        {
            {
                liboffkv::TxnCheck("/key", key_version),
                liboffkv::TxnCheck("/foo", foo_version),
                liboffkv::TxnCheck("/foo/bar", bar_version),
            },
            {
                liboffkv::TxnOpCreate("/key/child", "value"),
                liboffkv::TxnOpSet("/key", "new_value"),
                liboffkv::TxnOpErase("/foo"),
            }
        }
    ));

    ASSERT_TRUE(client->exists("/key/child"));
    ASSERT_EQ(client->get("/key").value, "new_value");
    ASSERT_FALSE(client->exists("/foo"));
    ASSERT_FALSE(client->exists("/foo/bar"));
    ASSERT_FALSE(client->exists("/foo/bar/subbar"));

    ASSERT_GT(result.at(1).version, key_version);
}

TEST_F(ClientFixture, erase_prefix_test)
{
    auto holder = hold_keys("/ichi", "/ichinichi");

    ASSERT_NO_THROW(client->create("/ichi",      "one"));
    ASSERT_NO_THROW(client->create("/ichinichi", "two"));

    ASSERT_NO_THROW(client->erase("/ichi"));

    ASSERT_TRUE(client->exists("/ichinichi"));
}

TEST_F(ClientFixture, get_prefix_test)
{
    auto holder = hold_keys("/sore", "/sorewanan");

    ASSERT_NO_THROW(client->create("/sore",      "1"));
    ASSERT_NO_THROW(client->create("/sore/ga",   "2"));
    ASSERT_NO_THROW(client->create("/sorewanan", "3"));

    liboffkv::ChildrenResult result;
    ASSERT_NO_THROW(result = client->get_children("/sore"));

    ASSERT_TRUE(liboffkv::detail::equal_as_unordered(
        client->get_children("/sore").children,
        {"/sore/ga"}
    ));
}
