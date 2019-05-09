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



template <class T>
T get_result(const std::unique_ptr<grpc::ClientAsyncResponseReader<T>> reader, grpc::CompletionQueue& queue, int k)
{
    T res;
    grpc::Status status;
    reader->Finish(&res, &status, &k);

    void* tag;
    bool ok;
    do {
        queue.Next(&tag, &ok);
    } while (!ok || tag != &k);

    if (status.ok())
        return res;

    std::cerr << status.error_message() << std::endl;
    throw std::runtime_error("Some problems");
}

zk::buffer zkbuf(const std::string& s) {
    return {s.begin(), s.end()};
}
std::string zkstr(const zk::buffer& s) {
    return {s.begin(), s.end()};
}

int main()
{
    zk::client client_ = zk::client::connect("zk://127.0.0.1:2181").get();

    zk::multi_op* txn = new zk::multi_op  {
        zk::op::set("be", zkbuf("mama")),
        zk::op::set("re", zkbuf("mia"))
    };
    client_.commit(*txn).get();
    delete txn;

    const zk::get_result zk_res1 = client_.get("/be").get();
    const zk::get_result zk_res2 = client_.get("/re").get();

    std::cout << "ZK: " << zkstr(zk_res1.data()) << ' ' << zkstr(zk_res2.data()) << std::endl;



    // ---- etcd ------
    grpc_init();

    std::shared_ptr<grpc::ClientContext> context = std::make_shared<grpc::ClientContext>();
    grpc::CompletionQueue queue;
    const std::shared_ptr<grpc::Channel>& chn = grpc::CreateChannel("localhost:2379",
                                                                    grpc::InsecureChannelCredentials());

    std::cerr << "Status 2" << std::endl;

    const std::unique_ptr<etcdserverpb::KV::Stub>& stub = etcdserverpb::KV::NewStub(chn);

    std::cerr << "Status 3" << std::endl;

    etcdserverpb::PutRequest preq;
    preq.set_key("fooq");
    preq.set_ignore_lease(false);
    preq.set_ignore_value(false);
    preq.set_value("test");
    preq.set_lease(0);

    std::unique_ptr<grpc::ClientAsyncResponseReader<etcdserverpb::PutResponse>> reader1 =
        stub->AsyncPut(context.get(), preq, &queue);
    etcdserverpb::PutResponse resp = get_result(std::move(reader1), queue, 1);

    context = std::make_shared<grpc::ClientContext>();

    std::cerr << "Status 4" << std::endl;

    etcdserverpb::RangeRequest rreq;
    rreq.set_key("fooq");
    rreq.set_limit(0);
    rreq.set_revision(-1);

    std::unique_ptr<grpc::ClientAsyncResponseReader<etcdserverpb::RangeResponse>> reader2 =
        stub->AsyncRange(context.get(), rreq, &queue);
    const etcdserverpb::RangeResponse response = get_result(std::move(reader2), queue, 2);

    std::cout << "Got keys: " << response.kvs_size() << std::endl;
    for (int i = 0; i < response.kvs_size(); ++i) {
        const mvccpb::KeyValue& kv = response.kvs(i);
        std::cout << kv.key() << " = " << kv.value() << std::endl;
    }
}