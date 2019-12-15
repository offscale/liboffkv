#pragma once

#include <string>
#include <memory>
#include "client.hpp"
#include "errors.hpp"
#include "util.hpp"
#include "key.hpp"

#include <liboffkv/config.hpp>

#ifdef ENABLE_CONSUL
#   include "consul_client.hpp"
#endif

#ifdef ENABLE_ETCD
#   include "etcd_client.hpp"
#endif

#ifdef ENABLE_ZK
#   include "zk_client.hpp"
#endif

namespace liboffkv {

std::unique_ptr<Client> open(std::string url, Path prefix = "")
{
    auto [protocol, address] = detail::split_url(url);

#ifdef ENABLE_ZK
    if (protocol == "zk")
        return std::make_unique<ZKClient>(std::move(url), std::move(prefix));
#endif

#ifdef ENABLE_CONSUL
    if (protocol == "consul")
        return std::make_unique<ConsulClient>(std::move(address), std::move(prefix));
#endif

#ifdef ENABLE_ETCD
    if (protocol == "etcd")
        return std::make_unique<ETCDClient>(std::move(address), std::move(prefix));
#endif

    throw InvalidAddress("protocol not supported: " + protocol);
}

} // namespace liboffkv
