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


    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return std::async(std::launch::async,
                          [this, key, value, lease] {
                              grpc::ClientContext context;

                              // put request
                              auto request = new PutRequest();
                              request->set_key(key);
                              request->set_value(value);
                              request->set_lease(lease);

                              TxnRequest txn;

                              // put condition: entry exists iff its create_revision > 0
                              auto cmp = txn.add_compare();
                              cmp->set_key(key);
                              cmp->set_target(Compare::CREATE);
                              cmp->set_result(Compare::EQUAL);
                              cmp->set_create_revision(0);

                              auto requestOp = txn.add_success();
                              requestOp->set_allocated_request_put(request);

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


    std::future<SetResult> set(const std::string& key, const std::string& value)
    {
        return std::async(std::launch::async,
                          [this, key, value]() -> SetResult  {
                              grpc::ClientContext context;

                              // put request
                              auto request = new PutRequest();
                              request->set_key(key);
                              request->set_value(value);

                              // range request after put to get key's version
                              auto get_request = new RangeRequest();
                              get_request->set_key(key);
                              get_request->set_limit(1);

                              TxnRequest txn;

                              auto cmp = txn.add_compare();
                              cmp->set_key(key);
                              cmp->set_target(Compare::CREATE);
                              cmp->set_result(Compare::GREATER);
                              cmp->set_create_revision(0);

                              auto requestOp_put = txn.add_success();
                              requestOp_put->set_allocated_request_put(request);

                              auto requestOp_get = txn.add_success();
                              requestOp_get->set_allocated_request_range(get_request);

                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded())
                                  throw NoEntry{};

                              return {response.mutable_responses(1)->release_response_range()->kvs(0).version()};
                          });
    }


    std::future<CASResult> cas(const std::string& key, const std::string& value, int64_t version)
    {
        return std::async(std::launch::async,
                          [this, key, value, version]() -> CASResult  {
                              if (version < int64_t(0))
                                  return {set(key, value).get().version, true};
                              grpc::ClientContext context;

                              // put request
                              auto request = new PutRequest();
                              request->set_key(key);
                              request->set_value(value);

                              // range request after put to get key's version
                              auto get_request = new RangeRequest();
                              get_request->set_key(key);
                              get_request->set_limit(1);

                              // range request after fail to determine real key's version
                              auto get_request_failure = new RangeRequest();
                              get_request_failure->set_key(key);
                              get_request_failure->set_limit(1);

                              TxnRequest txn;

                              auto cmp = txn.add_compare();
                              cmp->set_key(key);
                              cmp->set_target(Compare::VERSION);
                              cmp->set_result(Compare::EQUAL);
                              cmp->set_version(version);

                              auto requestOp_put = txn.add_success();
                              requestOp_put->set_allocated_request_put(request);

                              auto requestOp_get = txn.add_success();
                              requestOp_get->set_allocated_request_range(get_request);

                              auto requestOp_get_failure = txn.add_failure();
                              requestOp_get_failure->set_allocated_request_range(get_request_failure);

                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded())
                                  return {response.mutable_responses(0)->release_response_range()
                                                                       ->kvs(0).version(), false};

                              return {response.mutable_responses(1)->release_response_range()->kvs(0).version(), true};
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


    std::future<void> erase(const std::string& key, int64_t version)
    {
        return std::async(std::launch::async,
                          [this, key, version] {
                              grpc::ClientContext context;

                              auto request = new DeleteRangeRequest();
                              request->set_key(key);

                              TxnRequest txn;

                              if (version >= int64_t(0)) {
                                  auto cmp = txn.add_compare();
                                  cmp->set_key(key);
                                  cmp->set_target(Compare::CREATE);
                                  cmp->set_result(Compare::GREATER);
                                  cmp->set_create_revision(0);
                              }

                              auto requestOp = txn.add_success();
                              requestOp->set_allocated_request_delete_range(request);

                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded())
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
