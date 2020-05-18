#include <gtest/gtest.h>
#include <string>

#ifndef SERVICE_ADDRESS
#define SERVICE_ADDRESS ""
#endif

std::string server_addr;

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    server_addr = SERVICE_ADDRESS;
    for (int i = 1; i < argc; i++) {
        // FIXME: concat addresses when liboffkv starts supporting multiple urls
        server_addr = argv[i];
    }
    return RUN_ALL_TESTS();
}
