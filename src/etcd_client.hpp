#pragma once

#include <grpcpp/grpcpp.h>
#include <etcdpp/kv.pb.h>
#include <etcdpp/rpc.pb.h>
#include <etcdpp/rpc.grpc.pb.h>


#include "client_interface.hpp"



template <typename TimeMachine>
class ETCDClient : public Client<TimeMachine> {
private:
    using grpc::Channel;
    using grpc::ClientContext;
    using grpc::Status;
    using grpc::CompletionQueue;

    using KV = etcdserverpb::KV;

    using etcdserver::PutRequest;
    using etcdserver::PutResponse;
    using etcdserver::RangeRequest;
    using etcdserver::RangeResponse;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<RouteGuide::Stub> stub_;


    template <typename T>
    static
    T get_response(std::unique_ptr<grpc::ClientAsyncResponseReader<T>>&& reader, grpc::CompletionQueue& queue, int tag)
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

        throw status.error_message();
    }

public:
    ETCDClient(const std::string& address, std::shared_ptr<TimeMachine> tm = nullptr)
        : Client<TimeMachine>(address, std::move(tm)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KV::Stub::NewStub(channel_))
    {}

    ETCDClient() = delete;
    ETCDClient(const ETCDClient&) = delete;
    ETCDClient& operator=(const ETCDClient&) = delete;


    ETCDClient(ETCDClient&& another)
        : Client<TimeMachine>(std::move(another)),
          client_(std::move(another.client_)),
          stub_(KV::Stub::NewStub(channel_))
    {}

    ETCDClient& operator=(ETCDClient&& another)
    {
        client_ = std::move(another.client_);
        stub_ = KeyValueStore::NewStub(channel_);
        this->time_machine_ = std::move(another.time_machine_);

        return *this;
    }


    ~ETCDClient() = default;


    // TODO
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return std::async(std::launch::async,
                          [this, key, value, lease] {
                              try {
                                    //
                              } liboffkv_catch
                          });
    }


    std::future<ExistsResult> exists(const std::string& key) const
    {
        return std::async(std::launch::async,
                          [this, key]() -> ExistsResult {
                              try {
                                  ClientContext context;
                                  CompletionQueue queue;

                                  RangeRequest request;
                                  request.set_key(key);
                                  request.set_limit(1);

                                  auto response = get_response(stub_->AsyncRange(context.get(), request, &queue),
                                                               queue, 1);
                                  if (!response.kvs_size())
                                      return {-1, false};

                                  auto kv = response.kvs(0);
                                  return {kv.version(), true};
                              } liboffkv_catch
                          });
    }


    // TODO
    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        return std::async(std::launch::async,
                          [this, key, value]() -> SetResult {
                              try {
                                  return {};
                              } liboffkv_catch
                          });
    }


    // TODO
    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version = 0)
    {
        return std::async(std::launch::async,
                          [this, key]() -> CASResult {
                              try {
                                  return {};
                              } liboffkv_catch
                          });
    }


    std::future<GetResult> get(const std::string& key) const
    {
        return std::async(std::launch::async,
                          [this, key]() -> GetResult{
                              try {
                                  ClientContext context;
                                  CompletionQueue queue;

                                  RangeRequest request;
                                  request.set_key(key);
                                  request.set_limit(1);

                                  auto response = get_response(stub_->AsyncRange(context.get(), request, &queue),
                                                               queue, 1);
                                  if (!response.kvs_size())
                                      throw NoEntry{};

                                  auto kv = response.kvs(0);
                                  return {kv.version(), kv.value()};

                              } liboffkv_catch
                          });
    }


    // TODO: use version
    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        return std::async(std::launch::async,
                          [this, key]() -> ExistsResult {
                              try {
                                  ClientContext context;
                                  CompletionQueue queue;

                                  DeleteRangeRequest request;
                                  request.set_key(key);

                                  get_response(stub_->AsyncRange(context.get(), request, &queue), queue, 1);
                              } liboffkv_catch
                          });
    }

//    virtual
//    std::future<WatchResult> watch(const std::string& key) = 0;


    // TODO
    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        return std::async(std::launch::async,
                          [this, transaction]() -> TransactionResult {
                              try {
                                  return TransactionResult();
                              } liboffkv_catch
                          });
    }
};
