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



template <typename ThreadPool = time_machine::ThreadPool<>>
std::unique_ptr<Client<ThreadPool>> connect(const std::string& address, const std::string& prefix="",
                                             std::shared_ptr<ThreadPool> tm = nullptr)
{
    auto [protocol, host_port] = get_protocol_address(address);

#ifdef ENABLE_ZK
    if (protocol == "zk")
        return std::make_unique<ZKClient<ThreadPool>>(protocol + "://" + host_port, prefix, std::move(tm));
#endif

#ifdef ENABLE_CONSUL
    if (protocol == "consul")
        return std::make_unique<ConsulClient<ThreadPool>>(host_port, prefix, std::move(tm));
#endif

#ifdef ENABLE_ETCD
    if (protocol == "etcd")
        return std::make_unique<ETCDClient<ThreadPool>>(host_port, prefix, std::move(tm));
#endif

    throw InvalidAddress{};
}
