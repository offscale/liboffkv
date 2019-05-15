#pragma once

#include <grpcpp/grpcpp.h>
#include <etcdpp/kv.pb.h>
#include <etcdpp/rpc.pb.h>
#include <etcdpp/rpc.grpc.pb.h>
#include <grpcpp/security/credentials.h>

#include <future>
#include <mutex>


#include "client_interface.hpp"
#include "key.hpp"
#include "time_machine.hpp"



class ETCDHelper {
private:
    static constexpr size_t default_lease_timeout = 10; // seconds

    using LeaseGrantRequest = etcdserverpb::LeaseGrantRequest;
    using LeaseGrantResponse = etcdserverpb::LeaseGrantResponse;
    using LeaseKeepAliveRequest = etcdserverpb::LeaseKeepAliveRequest;
    using LeaseKeepAliveResponse = etcdserverpb::LeaseKeepAliveResponse;
    using LeaseEndpoint = etcdserverpb::Lease;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<LeaseEndpoint::Stub> lease_stub_;
    std::shared_ptr<time_machine::ThreadPool<>> thread_pool_;
    std::atomic<int64_t> lease_id_;
    std::mutex lock_;


    const void setup_lease_renewal(const int64_t lease_id, const uint64_t ttl) const
    {
        std::shared_ptr<grpc::ClientContext> ctx = std::make_shared<grpc::ClientContext>();
        std::shared_ptr<grpc::ClientReaderWriter<LeaseKeepAliveRequest, LeaseKeepAliveResponse>> stream =
            lease_stub_->LeaseKeepAlive(ctx.get());

        LeaseKeepAliveRequest req;
        req.set_id(lease_id);

        thread_pool_->periodic(
            [req = std::move(req), ctx = ctx, stream = std::move(stream)]() mutable {
                if (!stream->Write(req)) {
                    return std::chrono::seconds::zero();
                }
                LeaseKeepAliveResponse response;
                stream->Read(&response);
                return std::chrono::seconds((response.ttl() + 1) / 2);
            },
            std::chrono::seconds((ttl + 1) / 2)
        );
    }

    const int64_t create_lease() const
    {
        LeaseGrantRequest req;
        req.set_id(0);
        req.set_ttl(default_lease_timeout);

        grpc::ClientContext ctx;
        LeaseGrantResponse response;
        const grpc::Status status = lease_stub_->LeaseGrant(&ctx, req, &response);

        if (!status.ok())
            throw ServiceException(status.error_message());

        setup_lease_renewal(lease_id_, response.ttl());
        return response.id();
    }

public:
    ETCDHelper(const std::shared_ptr<grpc::Channel>& channel, std::shared_ptr<time_machine::ThreadPool<>> thread_pool)
        : channel_(channel), lease_stub_(LeaseEndpoint::NewStub(channel)), thread_pool_(std::move(thread_pool))
    {}

    const int64_t get_lease()
    {
        // double checked locking
        int64_t current = lease_id_.load(std::memory_order_relaxed);
        if (current) {
            return current;
        }

        std::lock_guard lock(lock_);

        current = lease_id_.load(std::memory_order_relaxed);
        if (current) {
            return current;
        }

        current = create_lease();
        lease_id_.store(current);
        return current;
    }
};


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
    std::string prefix_;

    ETCDHelper helper_;

    const std::string get_path(const std::string& key) const
    {
        // TODO modification for efficient hierarchy:
        // https://github.com/etcd-io/zetcd/blob/7f7c6911975f190f71df6ca2d56ae4a9f017c3b8/zketcd_path.go#L32
        return prefix_ + key;
    }

public:
    ETCDClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> tm)
        : Client(address, std::move(tm)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KV::NewStub(channel_)),
          prefix_(prefix),
          helper_(channel_, thread_pool_)
    {}

    ETCDClient() = delete;

    ETCDClient(const ETCDClient&) = delete;

    ETCDClient& operator=(const ETCDClient&) = delete;

    ETCDClient(ETCDClient&&) = delete;

    ETCDClient& operator=(ETCDClient&&) = delete;


    ~ETCDClient() = default;


    std::future<void> create(const std::string& key, const std::string& value, bool leased = false) override
    {
        return thread_pool_->async(
            [this, key = get_path(key), value, leased] {
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
                if (leased) {
                    request->set_lease(helper_.get_lease());
                } else {
                    request->set_lease(0);
                }

                auto requestOp_put = txn.add_success();
                requestOp_put->set_allocated_request_put(request);

                // on failure: get request to determine the kind of error
                auto get_request_failure = new RangeRequest();
                get_request_failure->set_key(key);
                get_request_failure->set_limit(1);
                get_request_failure->set_keys_only(true);


                TxnResponse response;

                auto status = stub_->Txn(&context, txn, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.succeeded()) {
                    if (response.mutable_responses(0)->release_response_range()->kvs_size()) {
                        throw EntryExists{};
                    }
                    // if compare failed but key does not exist the parent has to not exist
                    throw NoEntry{};
                }
            });
    }


    std::future<ExistsResult> exists(const std::string& key, bool watch = false) override
    {
        return thread_pool_->async(
            [this, key = get_path(key)]() -> ExistsResult {
                grpc::ClientContext context;

                RangeRequest request;
                request.set_key(key);
                request.set_limit(1);
                request.set_keys_only(true);
                RangeResponse response;

                auto status = stub_->Range(&context, request, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.kvs_size())
                    return {0, false};

                auto kv = response.kvs(0);
                return {static_cast<uint64_t>(kv.version()), true};
            });
    }


    std::future<ChildrenResult> get_children(const std::string& key, bool watch = false) override
    {
        // TODO
        return std::async(std::launch::async, [] { return ChildrenResult{}; });
    }


    std::future<SetResult> set(const std::string& key, const std::string& value) override
    {
        return thread_pool_->async(
            [this, key = get_path(key), value]() -> SetResult {
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
                // TODO investigate possible lease reset

                auto requestOp_put = txn.add_success();
                requestOp_put->set_allocated_request_put(request);

                // range request after put to get key's version
                auto get_request = new RangeRequest();
                get_request->set_key(key);
                get_request->set_limit(1);
                get_request->set_keys_only(true);

                auto requestOp_get = txn.add_success();
                requestOp_get->set_allocated_request_range(get_request);

                TxnResponse response;

                auto status = stub_->Txn(&context, txn, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.succeeded())
                    throw NoEntry{};

                return {
                    static_cast<uint64_t>(response.mutable_responses(1)->release_response_range()->kvs(0).version())
                };
            });
    }


    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) override
    {
        if (version == 0)
            return thread_pool_->then(create(key, value), [](std::future<void>&& res) -> CASResult {
                try {
                    res.get();
                    return {0, true};
                } catch (EntryExists&) {
                    return {0, false};
                }
            });

        return thread_pool_->async(
            [this, key = get_path(key), value, version]() -> CASResult {
                auto entries = get_entry_sequence(key);
                grpc::ClientContext context;

                TxnRequest txn;

                // check that given entry and its parent exist
//                for (size_t i = std::max<size_t>(0, entries.size() - 2); i < entries.size(); ++i) {
//                    auto cmp = txn.add_compare();
//                    cmp->set_key(entries[i]);
//                    cmp->set_target(Compare::CREATE);
//                    cmp->set_result(Compare::GREATER);
//                    cmp->set_create_revision(0);
//                }
                // checks above seem to be useless, comment

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
                request->set_ignore_lease(true);

                auto requestOp_put = txn.add_success();
                requestOp_put->set_allocated_request_put(request);

                // range request after put to get key's version
                auto get_request = new RangeRequest();
                get_request->set_key(key);
                get_request->set_limit(1);
                get_request->set_keys_only(true);

                auto requestOp_get = txn.add_success();
                requestOp_get->set_allocated_request_range(get_request);

                // on fail: check if the given key exists
                auto get_request_failure = new RangeRequest();
                get_request_failure->set_key(key);
                get_request_failure->set_limit(1);
                get_request_failure->set_keys_only(true);

                auto requestOp_get_failure = txn.add_failure();
                requestOp_get_failure->set_allocated_request_range(get_request_failure);


                TxnResponse response;

                auto status = stub_->Txn(&context, txn, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.succeeded()) {
                    auto failure_response = response.mutable_responses(0)->release_response_range();

                    if (failure_response->kvs_size())
                        return {static_cast<uint64_t>(failure_response->kvs(0).version()), false};

                    // ! throws NoEntry if version != 0 and key doesn't exist
                    throw NoEntry{};
                }

                return {
                    static_cast<uint64_t>(response.mutable_responses(1)->release_response_range()->kvs(0).version()),
                    true
                };
            });
    }


    std::future<GetResult> get(const std::string& key, bool watch = false) override
    {
        return thread_pool_->async(
            [this, key = get_path(key)]() -> GetResult {
                grpc::ClientContext context;

                RangeRequest request;
                request.set_key(key);
                request.set_limit(1);

                RangeResponse response;
                auto status = stub_->Range(&context, request, &response);

                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.kvs_size())
                    throw NoEntry{};

                auto kv = response.kvs(0);
                return {static_cast<uint64_t>(kv.version()), kv.value()};
            });
    }


    std::future<void> erase(const std::string& key, uint64_t version = 0)
    {
        return thread_pool_->async(
            [this, key = get_path(key), version] {
                grpc::ClientContext context;

                auto request = new DeleteRangeRequest();
                request->set_key(key);
                // TODO delete all subtree instead of just one key

                TxnRequest txn;

                if (version > 0) {
                    // check if versions are equal
                    auto cmp = txn.add_compare();
                    cmp->set_key(key);
                    cmp->set_target(Compare::VERSION);
                    cmp->set_result(Compare::EQUAL);
                    cmp->set_version(version);
                } else {
                    // check if key exists
                    auto cmp = txn.add_compare();
                    cmp->set_key(key);
                    cmp->set_target(Compare::CREATE);
                    cmp->set_result(Compare::GREATER);
                    cmp->set_create_revision(0);
                }

                auto requestOp = txn.add_success();
                requestOp->set_allocated_request_delete_range(request);

                // on failure: check if key exists
                auto get_request_failure = new RangeRequest();
                get_request_failure->set_key(key);
                get_request_failure->set_limit(1);
                get_request_failure->set_keys_only(true);

                auto requestOp_get_failure = txn.add_failure();
                requestOp_get_failure->set_allocated_request_range(get_request_failure);


                TxnResponse response;

                auto status = stub_->Txn(&context, txn, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.succeeded() && !response.mutable_responses(0)->release_response_range()->kvs_size())
                    throw NoEntry{};
            });
    }


    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        return thread_pool_->async(
            [this, transaction]() -> TransactionResult {
                grpc::ClientContext context;
                std::vector<int> create_indices, set_indices;
                int _i = 0;

                TxnRequest txn;

                for (const auto& check : transaction.checks()) {
                    auto cmp = txn.add_compare();
                    cmp->set_key(check.key);
                    cmp->set_target(Compare::VERSION);
                    cmp->set_result(Compare::EQUAL);
                    cmp->set_version(check.version);
                }

                for (const auto& op_ptr : transaction.operations()) {
                    switch (op_ptr->type) {
                        case op::op_type::CREATE: {
                            auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());

                            auto request = new PutRequest();
                            request->set_key(create_op_ptr->key);
                            request->set_value(create_op_ptr->value);

                            if (create_op_ptr->leased) {
                                request->set_lease(helper_.get_lease());
                            } else {
                                request->set_lease(0);
                            }

                            auto requestOP = txn.add_success();
                            requestOP->set_allocated_request_put(request);

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
                            get_request->set_key(set_op_ptr->key);
                            get_request->set_limit(1);

                            auto get_requestOP = txn.add_success();
                            get_requestOP->set_allocated_request_range(get_request);

                            create_indices.push_back(_i + 1);

                            _i += 2;

                            break;
                        }
                        case op::op_type::ERASE: {
                            auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());

                            auto request = new DeleteRangeRequest();
                            request->set_key(erase_op_ptr->key);

                            auto requestOP = txn.add_success();
                            requestOP->set_allocated_request_delete_range(request);

                            ++_i;

                            break;
                        }
                        default:
                            __builtin_unreachable();
                    }
                }

                TxnResponse response;

                auto status = stub_->Txn(&context, txn, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                if (!response.succeeded())
                    return TransactionResult();

                TransactionResult result;

                int i = 0, j = 0;
                while (i < create_indices.size() && j < set_indices.size())
                    if (create_indices[i] < set_indices[j]) {
                        result.push_back(CreateResult{});
                        ++i;
                    } else {
                        result.push_back(SetResult{static_cast<uint64_t>(
                                                       response.mutable_responses(
                                                               set_indices[j])->release_response_range()
                                                           ->kvs(0).version())});
                        ++j;
                    }

                while (i < create_indices.size()) {
                    result.push_back(CreateResult{});
                    ++i;
                }

                while (j < set_indices.size()) {
                    result.push_back(SetResult{static_cast<uint64_t>(
                                                   response.mutable_responses(set_indices[j])->release_response_range()
                                                       ->kvs(0).version())});
                    ++j;
                }

                return result;
            });
    }
};
