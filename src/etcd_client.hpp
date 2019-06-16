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


namespace liboffkv {

namespace helper {

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
    std::atomic<int64_t> lease_id_{0};
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
        thread_pool_->then(write_to_watch_stream_m(req, lock), util::call_get<void>);
    }

    bool process_watch_response_m(const WatchResponse& response)
    {
        const auto& events = response.events();

        auto handler_it = watch_handlers_.find(response.watch_id());
        if (handler_it == watch_handlers_.end()) {
            return true;
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
        bool do_join;
        {
            std::unique_lock lock(lock_);
            if (watch_stream_) {
                watch_context_->TryCancel();
            }
            cq_.Shutdown();
            do_join = watch_resolution_thread_running_;
        }
        if (do_join) {
            watch_resolution_thread_.join();
        }
    }
};


class ETCDTransactionBuilder {
private:
    using TxnRequest = etcdserverpb::TxnRequest;
    using Compare = etcdserverpb::Compare;
    using RequestOp = etcdserverpb::RequestOp;
    using PutRequest = etcdserverpb::PutRequest;
    using RangeRequest = etcdserverpb::RangeRequest;
    using DeleteRangeRequest = etcdserverpb::DeleteRangeRequest;

    TxnRequest txn_;


    Compare* make_compare_(const std::string& key, etcdserverpb::Compare_CompareTarget target,
                                                   etcdserverpb::Compare_CompareResult result)
    {
        auto cmp = txn_.add_compare();
        cmp->set_key(key);
        cmp->set_target(target);
        cmp->set_result(result);

        return cmp;
    }

    enum {
        UNDEFINED,
        SUCCESS,
        FAILURE,
    } status_;

public:
    ETCDTransactionBuilder()
    {}

    ~ETCDTransactionBuilder()
    {}


    TxnRequest& get_transaction()
    {
        return txn_;
    }

    ETCDTransactionBuilder& add_check_exists(const std::string& key)
    {
        if (key.size()) {
            auto cmp = make_compare_(key, Compare::CREATE, Compare::GREATER);
            cmp->set_create_revision(0);
        }

        return *this;
    }

    ETCDTransactionBuilder& add_check_not_exists(const std::string& key)
    {
        auto cmp = make_compare_(key, Compare::CREATE, Compare::EQUAL);
        cmp->set_create_revision(0);
        return *this;
    }

    ETCDTransactionBuilder& add_version_compare(const std::string& key, int64_t version)
    {
        auto cmp = make_compare_(key, Compare::VERSION, Compare::EQUAL);
        cmp->set_version(version);
        return *this;
    }

    ETCDTransactionBuilder& add_lease_compare(const std::string& key, int64_t lease)
    {
        auto cmp = make_compare_(key, Compare::LEASE, Compare::EQUAL);
        cmp->set_lease(lease);
        return *this;
    }

    ETCDTransactionBuilder& on_success()
    {
        status_ = SUCCESS;
        return *this;
    }

    ETCDTransactionBuilder& on_failure()
    {
        status_ = FAILURE;
        return *this;
    }

    ETCDTransactionBuilder& add_put_request(const std::string& key, const std::string& value,
                                            int64_t lease = 0, bool ignore_lease = false)
    {
        if (status_ == UNDEFINED)
            throw std::runtime_error("ETCDTransactionBuilder error: try to add request not having defined the branch "
                                     "(use on_success() or on_failure())");
        auto request = new PutRequest();
        request->set_key(key);
        request->set_value(value);

        if (ignore_lease)
            request->set_ignore_lease(true);
        else
            request->set_lease(lease);

        auto requestOp = status_ == SUCCESS ? txn_.add_success() : txn_.add_failure();
        requestOp->set_allocated_request_put(request);

        return *this;
    }

    ETCDTransactionBuilder& add_range_request(const std::string& key, bool keys_only = false,
                                              int64_t limit = 1, const std::string& range_end = {})
    {
        if (status_ == UNDEFINED)
            throw std::runtime_error("ETCDTransactionBuilder error: try to add request not having defined the branch "
                                     "(use on_success() or on_failure())");
        if (key.size()) {
            auto request = new RangeRequest();
            request->set_key(key);
            request->set_limit(limit);
            request->set_keys_only(keys_only);

            if (range_end.size())
                request->set_range_end(range_end);

            auto requestOp = status_ == SUCCESS ? txn_.add_success() : txn_.add_failure();
            requestOp->set_allocated_request_range(request);
        }

        return *this;
    }

    ETCDTransactionBuilder& add_delete_range_request(const std::string& key, const std::string& range_end = {})
    {
        if (status_ == UNDEFINED)
            throw std::runtime_error("ETCDTransactionBuilder error: try to add request not having defined the branch "
                                     "(use on_success() or on_failure())");
        auto request = new DeleteRangeRequest();
        request->set_key(key);

        if (range_end.size())
            request->set_range_end(range_end);

        auto requestOp = status_ == SUCCESS ? txn_.add_success() : txn_.add_failure();
        requestOp->set_allocated_request_delete_range(request);

        return *this;
    }
};

} // namespace helper


class ETCDClient : public Client {
private:
    using KV = etcdserverpb::KV;

    using RangeRequest = etcdserverpb::RangeRequest;
    using RangeResponse = etcdserverpb::RangeResponse;
    using TxnRequest = etcdserverpb::TxnRequest;
    using TxnResponse = etcdserverpb::TxnResponse;

    using Key = key::Key;
    using ETCDHelper = helper::ETCDHelper;
    using ETCDTransactionBuilder = helper::ETCDTransactionBuilder;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<KV::Stub> stub_;

    ETCDHelper helper_;

    static
    std::string key_transformer(const std::string& prefix, const std::vector<Key>& seq)
    {
        const std::string& key_parent = seq.size() > 1 ? seq[seq.size() - 2].get_raw_key() : "";
        const std::string key_base = seq.rbegin()->get_raw_key().substr(key_parent.size() + 1);
        return prefix + key_parent + '/' + static_cast<char>(0) + key_base;
    }

    static
    std::string children_key_transformer(const std::string& prefix, const std::vector<Key>& seq,
                                         bool direct_children_only, bool range_end)
    {
        char end = direct_children_only ? static_cast<char>(0) : '/';
        if (range_end)
            ++end;
        std::string tail = direct_children_only ? "/" : "";
        return prefix + seq.rbegin()->get_raw_key() + tail + end;
    }

    static
    Key::Transformer get_children_key_transformer(bool end)
    {
        using namespace std::placeholders;
        return std::bind(children_key_transformer, _1, _2, true, end);
    }

    static
    Key::Transformer erase_children_key_transformer(bool end)
    {
        using namespace std::placeholders;
        return std::bind(children_key_transformer, _1, _2, false, end);
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

    const std::string unwrap_key_(const std::string& full_path) const
    {
        size_t pos = full_path.rfind('/');
        return full_path.substr(prefix_.size(), pos + 1 - prefix_.size()) + full_path.substr(pos + 2);
    }

    TxnResponse commit_(grpc::ClientContext& context, const TxnRequest& txn)
    {
        TxnResponse response;

        auto status = stub_->Txn(&context, txn, &response);
        if (!status.ok())
            throw ServiceException(status.error_message());

        return response;
    }

public:
    ETCDClient(const std::string& address, const std::string& prefix, std::shared_ptr<ThreadPool> tm)
        : Client(address, prefix, std::move(tm)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KV::NewStub(channel_)),
          helper_(channel_, thread_pool_)
    {}

    ETCDClient() = delete;

    ETCDClient(const ETCDClient&) = delete;

    ETCDClient& operator=(const ETCDClient&) = delete;

    ETCDClient(ETCDClient&&) = delete;

    ETCDClient& operator=(ETCDClient&&) = delete;


    ~ETCDClient() = default;


    std::future<CreateResult> create(const std::string& key, const std::string& value, bool leased = false) override
    {
        return thread_pool_->async(
            [this, key = get_path_(key), value, leased]() -> CreateResult {
                grpc::ClientContext context;
                ETCDTransactionBuilder tb;

                tb.add_check_exists(key.get_parent())
                  .add_check_not_exists(key);

                // on success: put value
                // put request
                tb.on_success().add_put_request(key, value, leased ? helper_.get_lease() : 0);

                // on failure: get request to determine the kind of error
                tb.on_failure().add_range_request(key, true);

                TxnResponse response = commit_(context, tb.get_transaction());

                if (!response.succeeded()) {
                    if (response.mutable_responses(0)->release_response_range()->kvs_size()) {
                        throw EntryExists{};
                    }
                    // if compare failed but key does not exist the parent has to not exist
                    throw NoEntry{};
                }

                return {1};
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
                    watch_request.set_start_revision(response.header().revision() + 1);
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

            ETCDTransactionBuilder tb;
            tb.add_check_exists(key);

            // prepare keys
            auto key_begin = key.with_transformer(get_children_key_transformer(false));
            auto key_end = key.with_transformer(get_children_key_transformer(true));

            // on success: get all keys with appropriate prefix
            tb.on_success().add_range_request(key_begin, true, 0, key_end);

            TxnResponse response = commit_(context, tb.get_transaction());
            if (!response.succeeded())
                throw NoEntry{};

            ChildrenResult result;
            std::set<std::string> raw_keys;
            for (const auto& kv : response.mutable_responses(0)->release_response_range()->kvs()) {
                result.children.emplace_back(unwrap_key_(kv.key()));
                raw_keys.emplace(kv.key());
            }

            std::future<void> watch_future;
            if (watch) {
                auto watch_promise = std::make_shared<std::promise<void>>();
                watch_future = watch_promise->get_future();
                ETCDHelper::WatchCreateRequest watch_request;
                watch_request.set_key(key_begin);
                watch_request.set_range_end(key_end);
                watch_request.set_start_revision(response.header().revision() + 1);
                helper_.create_watch(watch_request, {
                    [promise = watch_promise, old_keys = std::move(raw_keys)](const ETCDHelper::Event& ev) {
                        bool old = old_keys.find(ev.kv().key()) != old_keys.end();
                        if ((old && ev.type() == ETCDHelper::EventType::Event_EventType_DELETE) ||
                               (!old && ev.type() == ETCDHelper::EventType::Event_EventType_PUT)) {
                            promise->set_value();
                            return true;
                        }
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
                bool expect_lease = true;

                while (true) {
                    expect_lease = !expect_lease;

                    grpc::ClientContext context;
                    ETCDTransactionBuilder tb;

                    auto parent = key.get_parent();

                    tb.add_check_exists(parent);
                    if (!expect_lease)
                        tb.add_lease_compare(key, 0);

                    // on success: put value and get new version
                    tb.on_success().add_put_request(key, value, 0, expect_lease)
                                   .add_range_request(key);

                    // on failure: range request to recognize failure reason
                    tb.on_failure().add_range_request(parent, true);

                    TxnResponse response = commit_(context, tb.get_transaction());
                    if (!response.succeeded()) {
                        auto& responses = *response.mutable_responses();
                        if (!responses.empty() && !responses[0].release_response_range()->kvs_size())
                            throw NoEntry{};

                        continue;
                    }

                    return {
                        static_cast<uint64_t>(response.mutable_responses(1)->release_response_range()->kvs(0).version())
                    };
                }
            });
    }


    std::future<CASResult> cas(const std::string& key, const std::string& value, uint64_t version = 0) override
    {
        if (version == 0)
            return thread_pool_->then(create(key, value), [](std::future<CreateResult>&& res) -> CASResult {
                try {
                    return {util::call_get(std::move(res)).version, true};
                } catch (EntryExists&) {
                    return {0, false};
                }
            });

        return thread_pool_->async(
            [this, key = get_path_(key), value, version]() -> CASResult {
                auto entries = key.get_sequence();
                grpc::ClientContext context;

                ETCDTransactionBuilder tb;

                tb.add_version_compare(key, version);

                // on success: put new value, get new version
                tb.on_success().add_put_request(key, value, 0, true)
                               .add_range_request(key, true);

                // on fail: check if the given key exists
                tb.on_failure().add_range_request(key, true);

                TxnResponse response = commit_(context, tb.get_transaction());
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
                    watch_request.set_start_revision(response.header().revision() + 1);
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
                ETCDTransactionBuilder tb;

                if (version > 0) {
                    // check if versions are equal
                    tb.add_version_compare(key, version);
                } else {
                    // check if key exists
                    tb.add_check_exists(key);
                }


                // prepare keys for erasing children
                auto key_begin = key.with_transformer(erase_children_key_transformer(false));
                auto key_end = key.with_transformer(erase_children_key_transformer(true));

                tb.on_success().add_delete_range_request(key)
                               .add_delete_range_request(key_begin, key_end);

                // on failure: check if key exists
                tb.on_failure().add_range_request(key, true);

                TxnResponse response = commit_(context, tb.get_transaction());
                if (!response.succeeded() && !response.mutable_responses(0)->release_response_range()->count())
                    throw NoEntry{};
            });
    }


    std::future<TransactionResult> commit(const Transaction& transaction)
    {
        return thread_pool_->async(
            [this, transaction]() -> TransactionResult {
                grpc::ClientContext context;

                std::vector<int> set_indices, create_indices;
                std::vector<std::vector<bool>> expected_existence;
                int _i = 0;

                ETCDTransactionBuilder tb;

                for (const auto& check : transaction.checks()) {
                    auto key = get_path_(check.key);
                    tb.add_version_compare(key, check.version)
                      .on_failure().add_range_request(key);
                }

                for (const auto& op_ptr : transaction.operations()) {
                    switch (op_ptr->type) {
                        case op::op_type::CREATE: {
                            auto create_op_ptr = dynamic_cast<op::Create*>(op_ptr.get());
                            auto key = get_path_(create_op_ptr->key);
                            auto parent = key.get_parent();
                            expected_existence.emplace_back();

                            tb.add_check_exists(parent)
                              .add_check_not_exists(key)
                              .on_success().add_put_request(key, create_op_ptr->value,
                                                            create_op_ptr->leased ? helper_.get_lease() : 0)
                              .on_failure().add_range_request(key, true);

                            expected_existence.back().push_back(false);
                            if (parent.size()) {
                                tb.on_failure().add_range_request(parent, true);
                                expected_existence.back().push_back(true);
                            }

                            create_indices.push_back(_i++);
                            break;
                        }
                        case op::op_type::SET: {
                            auto set_op_ptr = dynamic_cast<op::Set*>(op_ptr.get());
                            auto key = get_path_(set_op_ptr->key);
                            expected_existence.emplace_back();

                            tb.add_check_exists(key)
                              .on_success().add_put_request(key, set_op_ptr->value)
                                           .add_range_request(key)
                              .on_failure().add_range_request(key, true);

                            expected_existence.back().push_back(true);

                            set_indices.push_back(_i + 1);

                            _i += 2;

                            break;
                        }
                        case op::op_type::ERASE: {
                            auto erase_op_ptr = dynamic_cast<op::Erase*>(op_ptr.get());
                            auto key = get_path_(erase_op_ptr->key);
                            expected_existence.emplace_back();

                            tb.add_check_exists(key)
                              .on_success().add_delete_range_request(key)
                              .on_failure().add_range_request(key, true);

                            expected_existence.back().push_back(true);

                            ++_i;

                            break;
                        }
                    }
                }

                TxnResponse response = commit_(context, tb.get_transaction());

                if (!response.succeeded()) {
                    auto& responses = *response.mutable_responses();

                    for (size_t i = 0; i < transaction.checks().size(); ++i) {
                        auto resp_i = responses[i].release_response_range();
                        if (resp_i->kvs_size() == 0 || transaction.checks()[i].version != resp_i->kvs(0).version())
                            throw TransactionFailed{i};
                    }

                    size_t tr_checks_number = transaction.checks().size(), j = 0;
                    for (size_t i = 0; i < transaction.operations().size(); ++i)
                        for (bool should_exist : expected_existence[i]) {
                            if (should_exist ^ static_cast<bool>(responses[j + tr_checks_number]
                                                                    .release_response_range()->kvs_size()))
                                throw TransactionFailed{i + tr_checks_number};
                            ++j;
                        }

                    //
                    throw ServiceException{"transaction's failure is not connected with operations logic"};
                }

                TransactionResult result;

                int i = 0, j = 0;
                while (i < create_indices.size() && j < set_indices.size())
                    if (create_indices[i] < set_indices[j]) {
                        result.push_back(CreateResult{1});
                        ++i;
                    } else {
                        result.push_back(SetResult{static_cast<uint64_t>(
                                                       response.mutable_responses(
                                                               set_indices[j])->release_response_range()
                                                           ->kvs(0).version())});
                        ++j;
                    }

                while (i++ < create_indices.size())
                    result.push_back(CreateResult{1});

                while (j < set_indices.size())
                    result.push_back(SetResult{static_cast<uint64_t>(
                                                   response.mutable_responses(
                                                           set_indices[j++])->release_response_range()
                                                       ->kvs(0).version())});

                return result;
            });
    }
};

} // namespace liboffkv
