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
#include "operation.hpp"


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
    MoveTest a;
    std::future<void> future = a.doSmth();
    MoveTest b = std::move(a);
    future.get();
}
