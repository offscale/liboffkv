//
// Created by alloky on 11.06.19.
//

#include "client.hpp"
#include "key.hpp"
#include "operation.hpp"
#include "time_machine.hpp"
#include "util.hpp"

class SingleClientConnection {
public:
    SingleClientConnection()
    {
        timeMachine = std::make_shared<liboffkv::time_machine::ThreadPool<>>();
        std::string server_addr = SERVICE_ADDRESS;
        client = liboffkv::connect(server_addr, "/unitTests", timeMachine);
    }

    ~SingleClientConnection() = default;

    std::unique_ptr<liboffkv::Client> client;

private:
    std::shared_ptr<liboffkv::time_machine::ThreadPool<>> timeMachine;

};

