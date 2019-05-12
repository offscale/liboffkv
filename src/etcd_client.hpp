#pragma once

#include <grpcpp/grpcpp.h>
#include <etcdpp/kv.pb.h>
#include <etcdpp/rpc.pb.h>
#include <etcdpp/rpc.grpc.pb.h>
#include <grpcpp/security/credentials.h>


#include "client_interface.hpp"
#include "key.hpp"



class ETCDClient : public Client {
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
    ETCDClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> tm)
        : Client(address, std::move(tm)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KV::NewStub(channel_))
    {}

    ETCDClient() = delete;
    ETCDClient(const ETCDClient&) = delete;
    ETCDClient& operator=(const ETCDClient&) = delete;
    ETCDClient(ETCDClient&&) = delete;
    ETCDClient& operator=(ETCDClient&&) = delete;


    ~ETCDClient() = default;


    std::future<void> create(const std::string& key, const std::string& value, bool lease = false)
    {
        return this->thread_pool_->async(
                          [this, key, value, lease] {
                              auto entries = get_entry_sequence(key);
                              grpc::ClientContext context;

                              TxnRequest txn;

                              // check that parent exists
                              if (entries.size() >= 2) {
                                  auto cmp = txn.add_compare();
                                  cmp->set_key(entries[entries.size() - 2]);
                                  cmp->set_target(Compare::CREATE);
                                  cmp->set_result(Compare::GREATER);
                                  cmp->set_create_revision(0);
                              }

                              // check that entry to be created does not exist
                              auto cmp = txn.add_compare();
                              cmp->set_key(key);
                              cmp->set_target(Compare::CREATE);
                              cmp->set_result(Compare::EQUAL);
                              cmp->set_create_revision(0);

                              // on success: put value
                              // put request
                              auto request = new PutRequest();
                              request->set_key(key);
                              request->set_value(value);
                              request->set_lease(lease);

                              auto requestOp_put = txn.add_success();
                              requestOp_put->set_allocated_request_put(request);

                              // on failure: get request to determine the kind of error
                              auto get_request_failure = new RangeRequest();
                              get_request_failure->set_key(key);
                              get_request->set_limit(1);


                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded()) {
                                  if (response.mutable_responses(0)->release_response_range()->kvs_size()) {
                                      throw EntryExists{};
                                  }
                                  // if compare failed but key does not exist the parent has to not exist
                                  throw NoEntry{};
                              }
                          });
    }


    std::future<ExistsResult> exists(const std::string& key) const
    {
        return this->thread_pool_->async(
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
        return this->thread_pool_->async(
                          [this, key, value]() -> SetResult  {
                              auto entries = get_entry_sequence(key);
                              grpc::ClientContext context;

                              TxnRequest txn;

                              // check that parent exists
                              if (entries.size() >= 2) {
                                  auto cmp = txn.add_compare();
                                  cmp->set_key(entries[entries.size() - 2]);
                                  cmp->set_target(Compare::CREATE);
                                  cmp->set_result(Compare::GREATER);
                                  cmp->set_create_revision(0);
                              }

                              // on success: put value and get new version
                              // put request
                              auto request = new PutRequest();
                              request->set_key(key);
                              request->set_value(value);

                              auto requestOp_put = txn.add_success();
                              requestOp_put->set_allocated_request_put(request);

                              // range request after put to get key's version
                              auto get_request = new RangeRequest();
                              get_request->set_key(key);
                              get_request->set_limit(1);

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
        return this->thread_pool_->async(
                          [this, key, value, version]() -> CASResult  {
                              if (version == int64_t(0))
                                  try {
                                      create(key, value).get();
                                      return {0, true};
                                  } catch (EntryExists&) {
                                      return {0, false};
                                  }

                              auto entries = get_entry_sequence(key);
                              grpc::ClientContext context;


                              TxnRequest txn;

                              // check that given entry and its parent exist
                              for (size_t i = std::max(0, entries.size() - 2); i < entries.size(); ++i) {
                                  auto cmp = txn.add_compare();
                                  cmp->set_key(entries[i]);
                                  cmp->set_target(Compare::CREATE);
                                  cmp->set_result(Compare::GREATER);
                                  cmp->set_create_revision(0);
                              }

                              // check that versions are equal
                              auto cmp = txn.add_compare();
                              cmp->set_key(key);
                              cmp->set_target(Compare::VERSION);
                              cmp->set_result(Compare::EQUAL);
                              cmp->set_version(version);

                              // on success: put new value, get new version
                              // put request
                              auto request = new PutRequest();
                              request->set_key(key);
                              request->set_value(value);

                              auto requestOp_put = txn.add_success();
                              requestOp_put->set_allocated_request_put(request);

                              // range request after put to get key's version
                              auto get_request = new RangeRequest();
                              get_request->set_key(key);
                              get_request->set_limit(1);

                              auto requestOp_get = txn.add_success();
                              requestOp_get->set_allocated_request_range(get_request);

                              // on fail: check if the given key exists
                              auto get_request_failure = new RangeRequest();
                              get_request_failure->set_key(key);
                              get_request_failure->set_limit(1);

                              auto requestOp_get_failure = txn.add_failure();
                              requestOp_get_failure->set_allocated_request_range(get_request_failure);


                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded()) {
                                  auto failure_response = response.mutable_responses(0)->release_response_range();

                                  if (failure_response->kvs_size())
                                      return {failure_response->kvs(0).version(), false};

                                  // ! throws NoEntry if version != 0 and key doesn't exist
                                  return NoEntry{};
                              }

                              return {response.mutable_responses(1)->release_response_range()->kvs(0).version(), true};
                          });
    }


    std::future<GetResult> get(const std::string& key) const
    {
        return this->thread_pool_->async(
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
        return this->thread_pool_->async(
                          [this, key, version] {
                              grpc::ClientContext context;

                              auto request = new DeleteRangeRequest();
                              request->set_key(key);

                              TxnRequest txn;

                              auto cmp = txn.add_compare();
                              cmp->set_key(key);
                              cmp->set_target(Compare::CREATE);
                              cmp->set_result(Compare::GREATER);
                              cmp->set_create_revision(0);

                              if (version > int64_t(0)) {
                                  auto cmp = txn.add_compare();
                                  cmp->set_key(key);
                                  cmp->set_target(Compare::VERSION);
                                  cmp->set_result(Compare::EQUAL);
                                  cmp->set_version(version);
                              }

                              auto requestOp = txn.add_success();
                              requestOp->set_allocated_request_delete_range(request);

                              // on failure: check if key exists
                              auto get_request_failure = new RangeRequest();
                              get_request_failure->set_key(key);
                              get_request_failure->set_limit(1);

                              auto requestOp_get_failure = txn.add_failure();
                              requestOp_get_failure->set_allocated_request_range(get_request_failure);


                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded() && !response.mutable_responses(0)->release_response_range()
                                                                                         ->kvs_size())
                                  throw NoEntry{};
                          });
    }

//    virtual
//    std::future<WatchResult> watch(const std::string& key) = 0;


    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        return this->thread_pool_->async(
                          [this, transaction]() -> TransactionResult {
                              std::vector<int> create_indices, set_indices;
                              int _i = 0;

                              TxnRequest txn;

                              for (const auto& check : transaction.checks()) {
                                  auto cmp = txn.add_compare();
                                  cmp->set_target(Compare::VERSION);
                                  cmp->set_result(Compare::EQUAL);
                                  cmp->set_key(check.key);
                                  cmp->set_version(check.version);
                              }

                              for (const auto& op_ptr : transaction.operations()) {
                                  switch (op_ptr->type) {
                                      case op::op_type::CREATE: {
                                          auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());

                                          auto request = new PutRequest();
                                          request->set_key(create_op_ptr->key);
                                          request->set_value(create_op_ptr->value);

                                          auto requestOP = txn.add_success();
                                          requestOP->set_allocated_request_put(request);

                                          auto get_request = new RangeRequest();
                                          request->set_key(create_op_ptr->key);
                                          request->set_limit(1);

                                          auto get_requestOP = txn.add_success();
                                          get_requestOP->set_allocated_request_range(get_request);

                                          create_indices.push_back(_i + 1);

                                          _i += 2;
                                          break;
                                      }
                                      case op::op_type::SET: {
                                          auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());

                                          auto request = new PutRequest();
                                          request->set_key(set_op_ptr->key);
                                          request->set_value(set_op_ptr->value);

                                          auto requestOP = txn.add_success();
                                          requestOP->set_allocated_request_put(request);

                                          auto get_request = new RangeRequest();
                                          request->set_key(create_op_ptr->key);
                                          request->set_limit(1);

                                          auto get_requestOP = txn.add_success();
                                          get_requestOP->set_allocated_request_range(get_request);

                                          create_indices.push_back(_i + 1);

                                          _i += 2;

                                          break;
                                      }
                                      case op::op_type::ERASE: {
                                          auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());

                                          auto request = new DeleteRangeRequest();
                                          request->set_key(create_op_ptr->key);

                                          auto requestOP = txn.add_success();
                                          requestOP->set_allocated_request_delete_range(request);

                                          ++_i;

                                          break;
                                      }
                                      default: __builtin_unreachable();
                                  }
                              }

                              TxnResponse response;

                              auto status = stub_->Txn(&context, txn, &response);
                              if (!status.ok())
                                  throw status.error_message();

                              if (!response.succeeded())
                                  return TransactionResult();

                              TransactionResult result;

                              int i = 0, j = 0;
                              while (i < create_indices.size() && j < set_indices.size())
                                  if (create_indices[i] < set_indices[j]) {
                                      result.push_back(CreateResult{});
                                      ++i;
                                  } else {
                                      result.push_back(SetResult{response->get_mutable_responses(set_indices[j])
                                                                         ->release_response_range()->kvs(0).version()});
                                      ++j;
                                  }

                              while (i < create_indices.size()) {
                                  result.push_back(CreateResult{});
                                  ++i;
                              }

                              while (j < set_indices.size()) {
                                  result.push_back(SetResult{response->get_mutable_responses(set_indices[j])
                                                                 ->release_response_range()->kvs(0).version()});
                                  ++j;
                              }

                              return result;
                          });
    }
};
