#pragma once

#include <string>
#include "config.h"

#ifdef ENABLE_CONSUL
#include "consul_client.hpp"
#endif

#ifdef ENABLE_ETCD
#include "etcd_client.hpp"
#endif

#ifdef ENABLE_ZK
#include "zk_client.hpp"
#endif

#include "client_interface.hpp"
#include "error.hpp"
#include "time_machine.hpp"
#include "util.hpp"


namespace liboffkv {

std::unique_ptr<Client> connect(const std::string& address, const std::string& prefix="",
                                std::shared_ptr<time_machine::ThreadPool<>> tm = nullptr)
{
    auto [protocol, host_port] = util::get_protocol_address(address);

#ifdef ENABLE_ZK
    if (protocol == "zk")
        return std::make_unique<ZKClient>(protocol + "://" + host_port, prefix, std::move(tm));
#endif

#ifdef ENABLE_CONSUL
    if (protocol == "consul")
        return std::make_unique<ConsulClient>(host_port, prefix, std::move(tm));
#endif

#ifdef ENABLE_ETCD
    if (protocol == "etcd")
        return std::make_unique<ETCDClient>(host_port, prefix, std::move(tm));
#endif

    throw InvalidAddress(std::string("protocol \"") + protocol + "\" not allowed");
}

} // namespace liboffkv