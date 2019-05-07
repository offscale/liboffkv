#pragma once

#include <grpcpp/grpcpp.h>
#include <etcdpp/kv.pb.h>
#include <etcdpp/rpc.pb.h>
#include <etcdpp/rpc.grpc.pb.h>
#include <grpcpp/security/credentials.h>


#include "client_interface.hpp"



template <typename TimeMachine>
class ETCDClient : public Client<TimeMachine> {
private:
    using KV = etcdserverpb::KV;

    using PutRequest = etcdserverpb::PutRequest;
    using PutResponse = etcdserverpb::PutResponse;
    using RangeRequest = etcdserverpb::RangeRequest;
    using RangeResponse = etcdserverpb::RangeResponse;
    using DeleteRangeRequest = etcdserverpb::DeleteRangeRequest;
    using DeleteRangeResponse = etcdserverpb::DeleteRangeResponse;
    using TxnRequest = etcdserverpb::TxnRequest;
    using TxnResponse = etcdserverpb::TxnResponse;
    using Compare = etcdserverpb::Compare;
    using RequestOp = etcdserverpb::RequestOp;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<KV::Stub> stub_;


public:
    ETCDClient(const std::string& address, std::shared_ptr<TimeMachine> tm)
        : Client<TimeMachine>(address, std::move(tm)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KV::NewStub(channel_))
    {}

    ETCDClient() = delete;
    ETCDClient(const ETCDClient&) = delete;
    ETCDClient& operator=(const ETCDClient&) = delete;


    ETCDClient(ETCDClient&& another)
        : Client<TimeMachine>(std::move(another)),
          channel_(std::move(another.channel_)),
          stub_(KV::NewStub(channel_))
    {}

    ETCDClient& operator=(ETCDClient&& another)
    {
        channel_ = std::move(another.channel_);
        stub_ = KV::NewStub(channel_);
        this->time_machine_ = std::move(another.time_machine_);

        return *this;
    }


    ~ETCDClient() = default;


    // TODO
    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return std::async(std::launch::async,
                          [this, key, value, lease] {
                              grpc::ClientContext context;

                              Compare cmp;
                              cmp.set_key(key);
                              cmp.set_target(Compare::CREATE);
                              cmp.set_result(Compare::EQUAL);
                              cmp.set_create_revision(0);

                              PutRequest request;
                              request.set_key(key);
                              request.set_value(value);
                              request.set_lease(lease);

                              RequestOp requestOp;
                              requestOp.set_allocated_request_put(&request);

                              TxnRequest txn;
                              new (txn.add_compare()) Compare(std::move(cmp));
                              new (txn.add_success()) etcdserverpb::RequestOp(std::move(requestOp));
                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded())
                                  throw EntryExists{};
                          });
    }


    std::future<ExistsResult> exists(const std::string& key) const
    {
        return std::async(std::launch::async,
                          [this, key]() -> ExistsResult {
                              grpc::ClientContext context;

                              RangeRequest request;
                              request.set_key(key);
                              request.set_limit(1);
                              RangeResponse response;

                              auto status = stub_->Range(&context, request, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.kvs_size())
                                  return {-1, false};

                              auto kv = response.kvs(0);
                              return {kv.version(), true};
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
                              grpc::ClientContext context;

                              RangeRequest request;
                              request.set_key(key);
                              request.set_limit(1);

                              RangeResponse response;
                              auto status = stub_->Range(&context, request, &response);

                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.kvs_size())
                                  throw NoEntry{};

                              auto kv = response.kvs(0);
                              return {kv.version(), kv.value()};
                          });
    }


    // TODO: use version
    std::future<void> erase(const std::string& key, int64_t version = 0)
    {
        return std::async(std::launch::async,
                          [this, key] {
                              grpc::ClientContext context;

                              DeleteRangeRequest request;
                              request.set_key(key);

                              DeleteRangeResponse response;

                              auto status = stub_->DeleteRange(&context, request, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.deleted())
                                  throw NoEntry{};
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
