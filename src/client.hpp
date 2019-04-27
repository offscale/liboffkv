#pragma once

#include <string>


#include "consul_client.hpp"
#include "error.hpp"
#include "etcd_client.hpp"
#include "util.hpp"
#include "zk_client.hpp"




std::unique_ptr<Client> connect(const std::string& address)
{
    auto [protocol, host_port] = get_protocol_address(address);

    if (protocol == "zk")
        return std::make_unique<ZKClient>(protocol + "://" + host_port);
    if (protocol == "consul")
        return std::make_unique<ConsulClient>(host_port);
//    if (protocol == "etcd")
//        return std::make_unique<ETCDClient>(host_port);
    else
        throw InvalidAddress{}; 
}
