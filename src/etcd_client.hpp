#pragma once

#include <grpcpp/grpcpp.h>
#include <etcdpp/kv.pb.h>


#include "client_interface.hpp"



template <typename TimeMachine>
class ETCDClient : public Client<TimeMachine> {
private:
    using grpc::Channel;
    using grpc::ClientContext;
    using grpc::Status;

    // where are they?
    using keyvaluestore::KeyValueStore;
    using keyvaluestore::Request;
    using keyvaluestore::Response;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<RouteGuide::Stub> stub_;

public:
    ETCDClient(const std::string& address, std::shared_ptr<TimeMachine> tm = nullptr)
        : Client<TimeMachine>(address, std::move(tm)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KeyValueStore::NewStub(channel_))
    {}

    ETCDClient() = delete;
    ETCDClient(const ETCDClient&) = delete;
    ETCDClient& operator=(const ETCDClient&) = delete;


    ETCDClient(ETCDClient&& another)
        : Client<TimeMachine>(std::move(another)),
          client_(std::move(another.client_)),
          stub_(KeKeyValueStore::NewStub(channel_))
    {}

    ETCDClient& operator=(ETCDClient&& another)
    {
        client_ = std::move(another.client_);
        stub_ = KeyValueStore::NewStub(channel_);
        this->time_machine_ = std::move(another.time_machine_);

        return *this;
    }


    ~ETCDClient() = default;


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
                                  return {};
                              } liboffkv_catch
                          });
    }


    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        return std::async(std::launch::async,
                          [this, key, value]() -> SetResult {
                              try {
                                  return {};
                              } liboffkv_catch
                          });
    }


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
                                  auto stream = stub_->GetValues(&context);

                                  Request request;
                                  request.set_key(key);
                                  stream->Write(request);

                                  Response response;
                                  stream->Read(&response);

                                  stream->WritesDone();
                                  Status status = stream->Finish();


                                  if (!status.ok()) {
//                                      status.error_message();
//                                      status.error_code();
                                      throw "a certain etcd error";
                                  }

                                  // TODO: return version
                                  return {-1, response.value()};
                              } liboffkv_catch
                          });
    }


    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        return std::async(std::launch::async,
                          [this, key]() -> ExistsResult {
                              try {
                                  //
                              } liboffkv_catch
                          });
    }

//    virtual
//    std::future<WatchResult> watch(const std::string& key) = 0;


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
