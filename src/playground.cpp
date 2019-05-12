#include <future>
#include <thread>
#include <iostream>
#include <exception>
#include <memory>

#include <etcdpp/kv.pb.h>
#include <etcdpp/rpc.pb.h>
#include <etcdpp/rpc.grpc.pb.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <zk/client.hpp>
#include <zk/error.hpp>
#include <zk/multi.hpp>
#include <zk/types.hpp>


#include <ppconsul/consul.h>
#include <ppconsul/kv.h>



class MoveTest {
private:
    std::shared_ptr<int> shared_resource_;

public:
    MoveTest()
        : shared_resource_(std::make_shared<int>(5))
    {}

    MoveTest(MoveTest&& oth) noexcept
        : shared_resource_(std::move(oth.shared_resource_))
    {}


    std::future<void> doSmth() {
        return std::async(std::launch::async, [this]() {
           std::this_thread::sleep_for(std::chrono::seconds(1));
           std::cout << *shared_resource_ << std::endl;
        });
    }
};


int main()
{
    ppconsul::Consul consul;
    ppconsul::kv::Kv kv(consul);

    std::vector<ppconsul::kv::KeyValue> results = kv.commit({
        ppconsul::kv::TxnRequest::set("kek", "jopa"),
        ppconsul::kv::TxnRequest::get("kek"),
        ppconsul::kv::TxnRequest::set("keki", "wo"),
        ppconsul::kv::TxnRequest::erase("kek"),
        ppconsul::kv::TxnRequest::getAll("ke"),
                                              });

    for (const auto &kv : results) {
        printf("{key='%s', value='%s', session='%s', createIndex=%zu, modifyIndex=%zu, lockIndex=%zu, flags=%zu}\n",
               kv.key.c_str(),
               kv.value.c_str(),
               kv.session.c_str(),
               static_cast<size_t>(kv.createIndex),
               static_cast<size_t>(kv.modifyIndex),
               static_cast<size_t>(kv.lockIndex),
               static_cast<size_t>(kv.flags));
    }
}
