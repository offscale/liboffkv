
#pragma once

#include <string>


//#include "consul_client.hpp"
#include "error.hpp"
//#include "etcd_client.hpp"
#include "util.hpp"
#include "zk_client.hpp"
#include "time_machine.hpp"



template <typename TimeMachine = time_machine::TimeMachine<>>
std::unique_ptr<Client<TimeMachine>> connect(const std::string& address,
                                             std::shared_ptr<TimeMachine> tm = nullptr)
{
    auto [protocol, host_port] = get_protocol_address(address);

    if (protocol == "zk")
        return std::make_unique<ZKClient<TimeMachine>>(protocol + "://" + host_port, std::move(tm));
//    if (protocol == "consul")
//        return std::make_unique<ConsulClient>(host_port);
//    if (protocol == "etcd")
//        return std::make_unique<ETCDClient>(host_port);
    else
        throw InvalidAddress{};
}
