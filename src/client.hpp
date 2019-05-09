
#pragma once

#include <string>


#include "consul_client.hpp"
#include "error.hpp"
#include "etcd_client.hpp"
#include "time_machine.hpp"
#include "util.hpp"
#include "zk_client.hpp"



template <typename TimeMachine = time_machine::ThreadPool<>>
std::unique_ptr<Client<TimeMachine>> connect(const std::string& address,
                                             std::shared_ptr<TimeMachine> tm = nullptr)
{
    auto [protocol, host_port] = get_protocol_address(address);

    if (protocol == "zk")
        return std::make_unique<ZKClient<TimeMachine>>(protocol + "://" + host_port, std::move(tm));
    if (protocol == "consul")
        return std::make_unique<ConsulClient<TimeMachine>>(host_port, std::move(tm));
    if (protocol == "etcd")
        return std::make_unique<ETCDClient<TimeMachine>>(host_port, std::move(tm));
    else
        throw InvalidAddress{};
}
