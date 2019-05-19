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
public:
    using WatchCreateRequest = etcdserverpb::WatchCreateRequest;
    using WatchResponse = etcdserverpb::WatchResponse;
    using Event = mvccpb::Event;
    using EventType = mvccpb::Event_EventType;

    struct WatchHandler {
        std::function<bool(const Event&)> process_event;
        std::function<void(ServiceException)> process_failure;
    };

private:
    static constexpr size_t default_lease_timeout = 10; // seconds
    void* tag_init_stream = reinterpret_cast<void*>(1);
    void* tag_write_finished = reinterpret_cast<void*>(2);
    void* tag_response_got = reinterpret_cast<void*>(3);

    using LeaseGrantRequest = etcdserverpb::LeaseGrantRequest;
    using LeaseGrantResponse = etcdserverpb::LeaseGrantResponse;
    using LeaseKeepAliveRequest = etcdserverpb::LeaseKeepAliveRequest;
    using LeaseKeepAliveResponse = etcdserverpb::LeaseKeepAliveResponse;
    using WatchRequest = etcdserverpb::WatchRequest;
    using WatchCancelRequest = etcdserverpb::WatchCancelRequest;
    using LeaseEndpoint = etcdserverpb::Lease;
    using WatchEndpoint = etcdserverpb::Watch;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<LeaseEndpoint::Stub> lease_stub_;
    std::unique_ptr<WatchEndpoint::Stub> watch_stub_;
    std::shared_ptr<time_machine::ThreadPool<>> thread_pool_;
    std::atomic<int64_t> lease_id_;
    std::mutex lock_;

    grpc::CompletionQueue cq_;
    std::unique_ptr<grpc::ClientContext> watch_context_;
    bool watch_resolution_thread_running_ = false;
    std::thread watch_resolution_thread_;
    std::shared_ptr<grpc::ClientAsyncReaderWriter<WatchRequest, WatchResponse>> watch_stream_;
    std::shared_ptr<WatchResponse> pending_watch_response_;
    std::map<int64_t, WatchHandler> watch_handlers_;

    std::shared_ptr<std::promise<void>> current_watch_write_;
    std::condition_variable write_wait_cv_;
    std::shared_ptr<WatchHandler> pending_watch_handler_;
    std::condition_variable create_watch_wait_cv_;

    void fail_all_watches(const ServiceException& exc)
    {
        std::lock_guard lock(lock_);

        watch_stream_ = nullptr;
        pending_watch_response_ = nullptr;
        for (auto& handler_pair : watch_handlers_) {
            handler_pair.second.process_failure(exc);
        }
        watch_handlers_.clear();

        if (current_watch_write_) {
            current_watch_write_->set_exception(std::make_exception_ptr(exc));
            current_watch_write_ = nullptr;
        }

        if (pending_watch_handler_) {
            pending_watch_handler_->process_failure(exc);
            pending_watch_handler_ = nullptr;
        }

        write_wait_cv_.notify_all();
        create_watch_wait_cv_.notify_all();
    }

    void setup_watch_infrastructure_m()
    {
        if (watch_stream_)
            return;

        if (current_watch_write_)
            throw std::logic_error("Inconsistent internal state");

        current_watch_write_ = std::make_shared<std::promise<void>>(); // just to slow down next write until stream init
        watch_context_ = std::make_unique<grpc::ClientContext>();
        watch_stream_ = watch_stub_->AsyncWatch(watch_context_.get(), &cq_, tag_init_stream);

        if (!watch_resolution_thread_running_) {
            watch_resolution_thread_running_ = true;
            watch_resolution_thread_ = std::thread([this] {
                this->watch_resolution_loop();
            });
        }
    }

    void resolve_write_m()
    {
        current_watch_write_->set_value();
        current_watch_write_ = nullptr;
        write_wait_cv_.notify_one();
    }

    void watch_resolution_loop()
    {
        bool succeeded;
        void* tag;
        while (cq_.Next(&tag, &succeeded)) {
            if (!succeeded) {
                fail_all_watches(ServiceException{"Watch initialization failure"});
                continue;
            }

            std::unique_lock lock(lock_);

            if (tag == tag_init_stream) {
                // stream initialized
                resolve_write_m();
                request_read_next_watch_response_m();
                continue;
            }

            if (tag == tag_write_finished) {
                // resolve write and continue
                resolve_write_m();
                continue;
            }

            if (tag == tag_response_got) {
                // resolve response
                auto response = pending_watch_response_;
                pending_watch_response_ = nullptr;

                if (response) {
                    if (response->created()) {
                        watch_handlers_[response->watch_id()] = *pending_watch_handler_;
                        pending_watch_handler_ = nullptr;
                        create_watch_wait_cv_.notify_one();
                    }

                    if (!(response->created() || response->canceled()))
                        if (process_watch_response_m(*response)) {
                            cancel_watch_m(response->watch_id(), lock);
                        }
                }

                request_read_next_watch_response_m();
            }
        }
    }

    void cancel_watch_m(const int64_t watch_id, std::unique_lock<std::mutex>& lock)
    {
        auto* cancel_req = new WatchCancelRequest();
        cancel_req->set_watch_id(watch_id);
        WatchRequest req;
        req.set_allocated_cancel_request(cancel_req);
        thread_pool_->then(write_to_watch_stream_m(req, lock), call_get<void>);
    }

    const bool process_watch_response_m(const WatchResponse& response)
    {
        const auto& events = response.events();

        auto handler_it = watch_handlers_.find(response.watch_id());
        if (handler_it == watch_handlers_.end()) {
            return false;
        }
        WatchHandler& handler = handler_it->second;

        for (const Event& event : events) {
            if (handler.process_event(event)) {
                watch_handlers_.erase(response.watch_id());
                return true;
            }
        }

        return false;
    }

    void request_read_next_watch_response_m()
    {
        if (pending_watch_response_)
            throw std::logic_error("Inconsistent internal state");

        pending_watch_response_ = std::make_shared<WatchResponse>();
        watch_stream_->Read(pending_watch_response_.get(), tag_response_got);
    }

    std::future<void> write_to_watch_stream_m(const WatchRequest& request, std::unique_lock<std::mutex>& lock)
    {
        setup_watch_infrastructure_m();

        while (current_watch_write_) {
            write_wait_cv_.wait(lock);
        }

        current_watch_write_ = std::make_shared<std::promise<void>>();

        watch_stream_->Write(request, tag_write_finished);

        return current_watch_write_->get_future();
    }

    const void setup_lease_renewal_m(const int64_t lease_id, const uint64_t ttl) const
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

    const int64_t create_lease_m() const
    {
        LeaseGrantRequest req;
        req.set_id(0);
        req.set_ttl(default_lease_timeout);

        grpc::ClientContext ctx;
        LeaseGrantResponse response;
        const grpc::Status status = lease_stub_->LeaseGrant(&ctx, req, &response);

        if (!status.ok())
            throw ServiceException(status.error_message());

        setup_lease_renewal_m(lease_id_, response.ttl());
        return response.id();
    }

public:
    ETCDHelper(const std::shared_ptr<grpc::Channel>& channel, std::shared_ptr<time_machine::ThreadPool<>> thread_pool)
        : channel_(channel), lease_stub_(LeaseEndpoint::NewStub(channel)), watch_stub_(WatchEndpoint::NewStub(channel)),
          thread_pool_(std::move(thread_pool))
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

        current = create_lease_m();
        lease_id_.store(current);
        return current;
    }

    void create_watch(const WatchCreateRequest& create_req, const WatchHandler& handler)
    {
        WatchRequest request;
        request.set_allocated_create_request(new WatchCreateRequest(create_req));

        std::future<void> watch_write_future;

        {
            std::unique_lock lock(lock_);
            while (pending_watch_handler_) {
                create_watch_wait_cv_.wait(lock);
            }

            pending_watch_handler_ = std::make_shared<WatchHandler>(handler);
            watch_write_future = write_to_watch_stream_m(request, lock);
        }

        watch_write_future.get();
    }

    ~ETCDHelper()
    {
        std::unique_lock lock(lock_);
        if (watch_resolution_thread_running_) {
            cq_.Shutdown();
            lock.unlock();
            watch_resolution_thread_.join();
        }
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

    static
    std::string key_transformer(const std::string& prefix, const std::vector<Key>& seq)
    {
        return prefix + static_cast<char>(seq.size()) + seq.rbegin()->get_raw_key();
    }

    static
    std::string children_lookup_key_transformer(const std::string& prefix, const std::vector<Key>& seq)
    {
        return prefix + static_cast<char>(seq.size() + 1) + seq.rbegin()->get_raw_key() + '/';
    }

    static
    std::string children_lookup_end_key_transformer(const std::string& prefix, const std::vector<Key>& seq)
    {
        return prefix + static_cast<char>(seq.size() + 1) + seq.rbegin()->get_raw_key() + static_cast<char>('/' + 1);
    }

    static
    auto create_watch_failure_handler(const std::shared_ptr<std::promise<void>>& promise)
    {
        return [promise = promise](ServiceException exc) mutable {
            promise->set_exception(std::make_exception_ptr(exc));
        };
    }

    const Key get_path_(const std::string& key) const
    {
        Key res = key;
        res.set_prefix(prefix_);
        res.set_transformer(key_transformer);
        return res;
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
            [this, key = get_path_(key), value, leased] {
                auto entries = key.get_sequence();
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

                txn.add_failure()->set_allocated_request_range(get_request_failure);

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
            [this, key = get_path_(key), watch]() -> ExistsResult {
                grpc::ClientContext context;

                RangeRequest request;
                request.set_key(key);
                request.set_limit(1);
                request.set_keys_only(true);
                RangeResponse response;

                auto status = stub_->Range(&context, request, &response);
                if (!status.ok())
                    throw ServiceException(status.error_message());

                bool exists = response.kvs_size();

                std::future<void> watch_future;
                if (watch) {
                    auto watch_promise = std::make_shared<std::promise<void>>();
                    watch_future = watch_promise->get_future();
                    ETCDHelper::WatchCreateRequest watch_request;
                    watch_request.set_key(key);
                    watch_request.set_start_revision(response.header().revision());
                    helper_.create_watch(watch_request, {
                        [promise = watch_promise, exists](const ETCDHelper::Event& ev) {
                            if ((ev.type() == ETCDHelper::EventType::Event_EventType_DELETE && exists) ||
                                (ev.type() == ETCDHelper::EventType::Event_EventType_PUT && !exists)) {
                                promise->set_value();
                                return true;
                            }
                            return false;
                        },
                        create_watch_failure_handler(watch_promise)
                    });
                }

                if (!exists)
                    return {0, false, watch_future.share()};

                auto kv = response.kvs(0);
                return {static_cast<uint64_t>(kv.version()), true, watch_future.share()};
            });
    }


    std::future<ChildrenResult> get_children(const std::string& key, bool watch = false) override
    {
        return thread_pool_->async([this, key = get_path_(key), watch]() -> ChildrenResult {
            grpc::ClientContext context;

            TxnRequest txn;

            // check that parent exists
            auto cmp = txn.add_compare();
            cmp->set_key(key);
            cmp->set_target(Compare::CREATE);
            cmp->set_result(Compare::GREATER);
            cmp->set_create_revision(0);

            // prepare keys
            auto key_begin = key;
            auto key_end = key;
            key_begin.set_transformer(children_lookup_key_transformer);
            key_end.set_transformer(children_lookup_end_key_transformer);

            // on success: get all keys with appropriate prefix
            auto get_request = new RangeRequest();
            get_request->set_key(key_begin);
            get_request->set_range_end(key_end);
            get_request->set_keys_only(true);

            auto requestOp_get = txn.add_success();
            requestOp_get->set_allocated_request_range(get_request);

            TxnResponse response;

            auto status = stub_->Txn(&context, txn, &response);
            if (!status.ok())
                throw ServiceException(status.error_message());

            if (!response.succeeded())
                throw NoEntry{};

            ChildrenResult result;
            std::set<std::string> raw_keys;
            for (const auto& kv : response.mutable_responses(0)->release_response_range()->kvs()) {
                result.children.emplace_back(detach_prefix_(kv.key()));
                raw_keys.emplace(kv.key());
            }

            std::future<void> watch_future;
            if (watch) {
                auto watch_promise = std::make_shared<std::promise<void>>();
                watch_future = watch_promise->get_future();
                ETCDHelper::WatchCreateRequest watch_request;
                watch_request.set_key(key_begin);
                watch_request.set_range_end(key_end);
                watch_request.set_start_revision(response.header().revision());
                helper_.create_watch(watch_request, {
                    [promise = watch_promise, old_keys = std::move(raw_keys)](const ETCDHelper::Event& ev) {
                        bool old = old_keys.find(ev.kv().key()) != old_keys.end();
                        if ((old && ev.type() == ETCDHelper::EventType::Event_EventType_DELETE) ||
                            (!old && ev.type() == ETCDHelper::EventType::Event_EventType_PUT))
                            return true;
                        return false;
                    },
                    create_watch_failure_handler(watch_promise)
                });
            }
            result.watch = watch_future.share();
            return result;
        });
    }


    std::future<SetResult> set(const std::string& key, const std::string& value) override
    {
        return thread_pool_->async(
            [this, key = get_path_(key), value]() -> SetResult {
                auto entries = key.get_sequence();
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
            [this, key = get_path_(key), value, version]() -> CASResult {
                auto entries = key.get_sequence();
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
            [this, key = get_path_(key), watch]() -> GetResult {
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

                std::future<void> watch_future;
                if (watch) {
                    auto watch_promise = std::make_shared<std::promise<void>>();
                    watch_future = watch_promise->get_future();
                    ETCDHelper::WatchCreateRequest watch_request;
                    watch_request.set_key(key);
                    watch_request.set_start_revision(response.header().revision());
                    helper_.create_watch(watch_request, {
                        [promise = watch_promise](const ETCDHelper::Event& event) mutable {
                            promise->set_value();
                            return true;
                        },
                        create_watch_failure_handler(watch_promise)
                    });
                }

                auto kv = response.kvs(0);
                return {static_cast<uint64_t>(kv.version()), kv.value(), watch_future.share()};
            });
    }


    std::future<void> erase(const std::string& key, uint64_t version = 0)
    {
        return thread_pool_->async(
            [this, key = get_path_(key), version] {
                grpc::ClientContext context;

                auto request = new DeleteRangeRequest();
                request->set_key(key);

                // prepare keys
                auto key_begin = key;
                auto key_end = key;
                key_begin.set_transformer(children_lookup_key_transformer);
                key_end.set_transformer(children_lookup_end_key_transformer);

                auto chilren_request = new DeleteRangeRequest();
                chilren_request->set_key(key_begin);
                chilren_request->set_range_end(key_end);

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

                requestOp = txn.add_success();
                requestOp->set_allocated_request_delete_range(chilren_request);

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
                std::vector<int> set_indices;
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

                            set_indices.push_back(_i + 1);

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
                    throw TransactionFailed{};

                TransactionResult result;

                for (int j = 0; j < set_indices.size(); ++j)
                    result.push_back(SetResult{static_cast<uint64_t>(
                                                   response.mutable_responses(set_indices[j])->release_response_range()
                                                       ->kvs(0).version())});

                return result;
            });
    }
};
