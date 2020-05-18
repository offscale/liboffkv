#include "test_client_fixture.hpp"
#include "testenv.hpp"
#include <liboffkv/liboffkv.hpp>

TEST_F(ClientFixture, conn_failure_test) {
    client->create("/key", "value");

    auto result = client->exists("/key");
    ASSERT_TRUE(result);

    ConnectionControl::instance().disconnect();

    ASSERT_THROW(client->erase("/key"), liboffkv::ConnectionLoss);
}
