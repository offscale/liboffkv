#pragma once

#include <grpcpp/grpcpp.h>
#include <libetcd/kv.pb.h>
#include <libetcd/rpc.pb.h>
#include <libetcd/rpc.grpc.pb.h>
#include <grpcpp/security/credentials.h>

#include <future>
#include <mutex>


#include "key.hpp"



namespace liboffkv {


class ETCDWatchCreator {
public:
    using WatchCreateRequest = etcdserverpb::WatchCreateRequest;
    using WatchResponse = etcdserverpb::WatchResponse;
    using Event = mvccpb::Event;
    using EventType = mvccpb::Event_EventType;

    struct WatchEventHandler {
        std::function<bool(const Event&)> process_event;
        std::function<void(ServiceError)> process_failure;
    };

private:
    void* tag_init_stream = reinterpret_cast<void*>(1);
    void* tag_write_finished = reinterpret_cast<void*>(2);
    void* tag_response_got = reinterpret_cast<void*>(3);

    using WatchRequest = etcdserverpb::WatchRequest;
    using WatchCancelRequest = etcdserverpb::WatchCancelRequest;
    using WatchEndpoint = etcdserverpb::Watch;

    std::mutex lock_;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr <WatchEndpoint::Stub> watch_stub_;
    std::unique_ptr <grpc::ClientContext> watch_context_;
    grpc::CompletionQueue cq_;

    bool watch_resolution_thread_running_ = false;
    std::thread watch_resolution_thread_;
    std::shared_ptr <grpc::ClientAsyncReaderWriter<WatchRequest, WatchResponse>> watch_stream_;
    std::shared_ptr <WatchResponse> pending_watch_response_;
    std::map <int64_t, WatchEventHandler> watch_handlers_;

    std::shared_ptr <std::promise<void>> current_watch_write_;
    std::condition_variable write_wait_cv_;
    std::shared_ptr <WatchEventHandler> pending_watch_handler_;
    std::condition_variable create_watch_wait_cv_;

    void fail_all_watches(const ServiceError& exc)
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

        current_watch_write_ =
            std::make_shared < std::promise < void >> (); // just to slow down next write until stream init
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
                fail_all_watches(ServiceError{"Watch initialization failure"});
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

    void cancel_watch_m(const int64_t watch_id, std::unique_lock <std::mutex>& lock)
    {
        auto* cancel_req = new WatchCancelRequest();
        cancel_req->set_watch_id(watch_id);
        WatchRequest req;
        req.set_allocated_cancel_request(cancel_req);
        write_to_watch_stream_m(req, lock);
    }

    bool process_watch_response_m(const WatchResponse& response)
    {
        const auto& events = response.events();

        auto handler_it = watch_handlers_.find(response.watch_id());
        if (handler_it == watch_handlers_.end()) {
            return true;
        }
        WatchEventHandler& handler = handler_it->second;

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

    std::future<void> write_to_watch_stream_m(const WatchRequest& request, std::unique_lock <std::mutex>& lock)
    {
        setup_watch_infrastructure_m();

        while (current_watch_write_) {
            write_wait_cv_.wait(lock);
        }

        current_watch_write_ = std::make_shared<std::promise<void>>();

        watch_stream_->Write(request, tag_write_finished);

        return current_watch_write_->get_future();
    }


public:
    ETCDWatchCreator(const std::shared_ptr<grpc::Channel>& channel)
        : channel_(channel), watch_stub_(WatchEndpoint::NewStub(channel))
    {}

    void create_watch(const WatchCreateRequest& create_req, const WatchEventHandler& handler)
    {
        WatchRequest request;
        request.set_allocated_create_request(new WatchCreateRequest(create_req));

        std::future<void> watch_write_future;

        {
            std::unique_lock lock(lock_);
            while (pending_watch_handler_) {
                create_watch_wait_cv_.wait(lock);
            }

            pending_watch_handler_ = std::make_shared<WatchEventHandler>(handler);
            watch_write_future = write_to_watch_stream_m(request, lock);
        }

        watch_write_future.get();
    }

    ~ETCDWatchCreator()
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


class LeaseIssuer {
private:
    static constexpr size_t LEASE_TIMEOUT = 10; // seconds

    using LeaseEndpoint = etcdserverpb::Lease;

    std::atomic <int64_t> lease_id_{0};
    std::shared_ptr <grpc::Channel> channel_;
    std::unique_ptr <LeaseEndpoint::Stub> lease_stub_;
    detail::PingSender ping_sender_;
    std::mutex lock_;


    void setup_lease_renewal_(int64_t lease_id, std::chrono::seconds ttl)
    {
        std::shared_ptr<grpc::ClientContext> context_ptr;
        auto stream = lease_stub_->LeaseKeepAlive(context_ptr.get());

        etcdserverpb::LeaseKeepAliveRequest req;
        req.set_id(lease_id);

        ping_sender_ = detail::PingSender(
            (ttl + std::chrono::seconds(1)) / 2,
            [req = std::move(req), context_ptr, stream = std::move(stream)]() {
                if (!stream->Write(req)) {
                    return std::chrono::seconds::zero();
                }
                etcdserverpb::LeaseKeepAliveResponse response;
                stream->Read(&response);
                return std::chrono::seconds((response.ttl() + 1) / 2);
            }
        );
    }

    int64_t create_lease_()
    {
        etcdserverpb::LeaseGrantRequest req;
        req.set_id(0);
        req.set_ttl(LEASE_TIMEOUT);

        grpc::ClientContext context;
        etcdserverpb::LeaseGrantResponse response;
        grpc::Status status = lease_stub_->LeaseGrant(&context, req, &response);

        if (!status.ok())
            throw ServiceError(status.error_message());

        setup_lease_renewal_(response.id(), std::chrono::seconds(response.ttl()));
        return response.id();
    }


public:
    LeaseIssuer(const std::shared_ptr<grpc::Channel>& channel)
        : channel_(channel), lease_stub_(LeaseEndpoint::NewStub(channel))
    {}

    int64_t get_lease()
    {
        // double checked locking
        int64_t current = lease_id_.load(std::memory_order_relaxed);
        if (current) {
            return current;
        }

        std::lock_guard lock(lock_);
        if (current = lease_id_.load(std::memory_order_relaxed)) {
            return current;
        }

        current = create_lease_();
        lease_id_.store(current);
        return current;
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

    template <typename Request>
    static
    void set_key_range_(Request* request, const std::variant<std::string, std::pair<std::string, std::string>>& range)
    {
        std::visit([&request](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;

            if constexpr (std::is_same_v<T, std::string>) {
                if (arg.size()) request->set_key(arg);
            } else /* blablabla */ {
                auto [begin, end] = arg;
                request->set_key(begin);
                request->set_range_end(end);
            }
        }, range);
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

    ETCDTransactionBuilder& add_range_request(const std::variant<std::string, std::pair<std::string, std::string>>& range,
                                              bool keys_only = false, int64_t limit = 1)
    {
        if (status_ == UNDEFINED)
            throw std::runtime_error("ETCDTransactionBuilder error: try to add request not having defined the branch "
                                     "(use on_success() or on_failure())");
        auto request = new RangeRequest();

        set_key_range_(request, range);
        request->set_limit(limit);
        request->set_keys_only(keys_only);

        auto requestOp = status_ == SUCCESS ? txn_.add_success() : txn_.add_failure();
        requestOp->set_allocated_request_range(request);

        return *this;
    }

    ETCDTransactionBuilder& add_delete_range_request(const std::variant<std::string, std::pair<std::string, std::string>>& range)
    {
        if (status_ == UNDEFINED)
            throw std::runtime_error("ETCDTransactionBuilder error: try to add request not having defined the branch "
                                     "(use on_success() or on_failure())");
        auto request = new DeleteRangeRequest();
        set_key_range_(request, range);

        auto requestOp = status_ == SUCCESS ? txn_.add_success() : txn_.add_failure();
        requestOp->set_allocated_request_delete_range(request);

        return *this;
    }
};



class ETCDClient : public Client {
private:
    using KV = etcdserverpb::KV;

    using RangeRequest = etcdserverpb::RangeRequest;
    using RangeResponse = etcdserverpb::RangeResponse;
    using TxnRequest = etcdserverpb::TxnRequest;
    using TxnResponse = etcdserverpb::TxnResponse;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<KV::Stub> stub_;

    LeaseIssuer lease_issuer_;
    ETCDWatchCreator watch_creator_;


    const std::string unwrap_key_(const std::string& full_path) const
    {
        size_t pos = full_path.rfind('/');
        auto prefix_size = as_path_string_(Path{""}).size();
        return full_path.substr(prefix_size, pos + 1 - prefix_size) + full_path.substr(pos + 2);
    }

    TxnResponse commit_(grpc::ClientContext& context, const TxnRequest& txn)
    {
        TxnResponse response;

        auto status = stub_->Txn(&context, txn, &response);
        if (!status.ok())
            throw ServiceError(status.error_message());

        return response;
    }

    std::string as_path_string_(const Path &path) const
    {
        return static_cast<std::string>(prefix_ / path);
    }


    class ETCDWatchHandle_ : public WatchHandle {
    private:
        std::future<void> future_;

    public:
        ETCDWatchHandle_(std::future<void>&& future)
            : future_(std::move(future))
        {}

        void wait() override
        {
            future_.get();
        }
    };


    template <typename EventChecker>
    std::unique_ptr<WatchHandle> make_watch_handle_(
            const ETCDWatchCreator::WatchCreateRequest& request,
            EventChecker&& stop_waiting_condition
        )
    {
        std::promise<void> promise;
        watch_creator_.create_watch(
            request,
            {
                [&promise,
                    foo = std::forward<EventChecker>(stop_waiting_condition)](const ETCDWatchCreator::Event& event) mutable {
                    if (foo(event)) {
                        promise.set_value();
                        return true;
                    }
                    return false;
                },
                [&promise](const ServiceError& exc)
                {
                    promise.set_exception(std::make_exception_ptr(exc));
                }
            });
        return std::make_unique<ETCDWatchHandle_>(promise.get_future());
    }


public:
    ETCDClient(const std::string& address, Path prefix)
        : Client(std::move(prefix)),
          channel_(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
          stub_(KV::NewStub(channel_)),
          watch_creator_(channel_),
          lease_issuer_(channel_)
    {}


    int64_t create(const Key& key, const std::string& value, bool lease = false) override
    {
        grpc::ClientContext context;
        ETCDTransactionBuilder bldr;

        auto path = as_path_string_(key);

        bldr.add_check_exists(as_path_string_(key.parent()))
            .add_check_not_exists(path)
            // on success put value
            .on_success().add_put_request(path, value, lease ? lease_issuer_.get_lease() : 0)
            // on failure perform get request to determine a kind of error
            .on_failure().add_range_request(path, true);

        TxnResponse response = commit_(context, bldr.get_transaction());
        if (!response.succeeded()) {
            if (response.mutable_responses(0)->release_response_range()->kvs_size()) {
                throw EntryExists{};
            }
            // if compare failed but key does not exist the parent has to not exist
            throw NoEntry{};
        }

        return 1;
    }


    ExistsResult exists(const Key& key, bool watch = false) override
    {
        grpc::ClientContext context;
        auto path = as_path_string_(key);

        RangeRequest request;
        request.set_key(path);
        request.set_limit(1);
        request.set_keys_only(true);
        RangeResponse response;

        auto status = stub_->Range(&context, request, &response);
        if (!status.ok())
            throw ServiceError(status.error_message());

        bool exists = response.kvs_size();

        std::unique_ptr<WatchHandle> watch_handle;
        if (watch) {
            ETCDWatchCreator::WatchCreateRequest watch_request;
            watch_request.set_key(path);
            watch_request.set_start_revision(response.header().revision() + 1);
            watch_handle = make_watch_handle_(watch_request, [exists](const ETCDWatchCreator::Event& ev) {
                return (ev.type() == ETCDWatchCreator::EventType::Event_EventType_DELETE && exists) ||
                       (ev.type() == ETCDWatchCreator::EventType::Event_EventType_PUT && !exists);
            });
        }

        return {
            !exists ? 0 : static_cast<int64_t>(response.kvs(0).version()),
            std::move(watch_handle)
        };
    }


    ChildrenResult get_children(const Key& key, bool watch = false) override
    {
        grpc::ClientContext context;

        // children range
        auto key_begin = as_path_string_(key / std::string(1, static_cast<char>(0)));
        auto key_end   = as_path_string_(key / std::string(1, static_cast<char>(0) + 1));

        ETCDTransactionBuilder bldr;
        bldr.add_check_exists(as_path_string_(key))
            .on_success().add_range_request(std::make_pair(key_begin, key_end), true, 0);

        TxnResponse response = commit_(context, bldr.get_transaction());
        if (!response.succeeded())
            throw NoEntry{};

        std::vector<std::string> children;
        std::set<std::string> raw_keys;
        for (const auto& kv : response.mutable_responses(0)->release_response_range()->kvs()) {
            children.emplace_back(unwrap_key_(kv.key()));
            raw_keys.emplace(kv.key());
        }

        std::unique_ptr<WatchHandle> watch_handle;
        if (watch) {
            ETCDWatchCreator::WatchCreateRequest watch_request;
            watch_request.set_key(key_begin);
            watch_request.set_range_end(key_end);
            watch_request.set_start_revision(response.header().revision() + 1);
            watch_handle = make_watch_handle_(watch_request, [old_keys = std::move(raw_keys)](const ETCDWatchCreator::Event& ev) {
                bool old = old_keys.find(ev.kv().key()) != old_keys.end();
                return (old && ev.type() == ETCDWatchCreator::EventType::Event_EventType_DELETE) ||
                       (!old && ev.type() == ETCDWatchCreator::EventType::Event_EventType_PUT);
            });
        }
        return { std::move(children), std::move(watch_handle) };
    }


    int64_t set(const Key& key, const std::string& value) override
    {
        // used to preserve lease(less)ness
        bool expect_lease = true;

        while (true) {
            expect_lease = !expect_lease;

            grpc::ClientContext context;
            ETCDTransactionBuilder bldr;

            auto parent = as_path_string_(key.parent());
            auto path = as_path_string_(key);

            bldr.add_check_exists(parent);
            if (!expect_lease) bldr.add_lease_compare(path, 0);

            bldr.on_success().add_put_request(path, value, 0, expect_lease)
                             .add_range_request(path)
                .on_failure().add_range_request(parent, true);

            TxnResponse response = commit_(context, bldr.get_transaction());
            if (!response.succeeded()) {
                auto& responses = *response.mutable_responses();
                if (!responses.empty() && !responses[0].release_response_range()->kvs_size())
                    throw NoEntry{};

                continue;
            }

            return response.mutable_responses(1)->release_response_range()->kvs(0).version();
        }
    }


    CasResult cas(const Key& key, const std::string& value, int64_t version = 0) override
    {
        if (!version) {
            try {
                return {create(key, value)};
            } catch (EntryExists&) {
                return {0};
            }
        }

        auto path = as_path_string_(key);
        grpc::ClientContext context;

        ETCDTransactionBuilder bldr;

        bldr.add_version_compare(path, version)
            .on_success().add_put_request(path, value, 0, true)
                         .add_range_request(path, true)
            .on_failure().add_range_request(path, true);

        TxnResponse response = commit_(context, bldr.get_transaction());
        if (!response.succeeded()) {
            auto failure_response = response.mutable_responses(0)->release_response_range();
            if (failure_response->kvs_size())
                return {failure_response->kvs(0).version()};

            // ! throw NoEntry if version != 0 and key doesn't exist
            throw NoEntry{};
        }

        return {response.mutable_responses(1)->release_response_range()->kvs(0).version()};
    }


    GetResult get(const Key& key, bool watch = false) override
    {
        grpc::ClientContext context;
        auto path = as_path_string_(key);

        RangeRequest request;
        request.set_key(path);
        request.set_limit(1);

        RangeResponse response;
        auto status = stub_->Range(&context, request, &response);

        if (!status.ok())
            throw ServiceError(status.error_message());

        if (!response.kvs_size())
            throw NoEntry{};

        std::unique_ptr<WatchHandle> watch_handle;
        if (watch) {
            ETCDWatchCreator::WatchCreateRequest watch_request;
            watch_request.set_key(path);
            watch_request.set_start_revision(response.header().revision() + 1);

            watch_handle = make_watch_handle_(watch_request, [](const ETCDWatchCreator::Event&) { return true; });
        }

        auto kv = response.kvs(0);
        return { static_cast<int64_t>(kv.version()), kv.value(), std::move(watch_handle) };
    }


    void erase(const Key& key, int64_t version = 0) override
    {
        auto path = as_path_string_(key);

        grpc::ClientContext context;
        ETCDTransactionBuilder bldr;

        if (version) bldr.add_version_compare(path, version);
        else         bldr.add_check_exists(path);

        bldr.on_success().add_delete_range_request(path)
                         .add_delete_range_request(std::make_pair(
                             as_path_string_(key / std::string(1, '/')),
                             as_path_string_(key / std::string(1, '/' + 1))
                         ))
            .on_failure().add_range_request(path, true);

        TxnResponse response = commit_(context, bldr.get_transaction());
        if (!response.succeeded() && !response.mutable_responses(0)->release_response_range()->count())
            throw NoEntry{};
    }


    TransactionResult commit(const Transaction& transaction) override
    {
        grpc::ClientContext context;

        std::vector<int> set_indices, create_indices;
        std::vector<std::vector<bool>> expected_existence;
        int _i = 0;

        ETCDTransactionBuilder bldr;

        for (const auto& check : transaction.checks) {
            auto path = as_path_string_(check.key);
            bldr.add_version_compare(path, check.version)
                .on_failure().add_range_request(path);
        }

        for (const auto& op : transaction.ops) {
            std::visit([this, &expected_existence, &create_indices, &set_indices, &_i, &bldr](auto&& arg) {
                auto path = as_path_string_(arg.key);
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, TxnOpCreate>) {
                    expected_existence.emplace_back();
                    bldr.add_check_not_exists(path)
                        .on_success().add_put_request(path, arg.value,
                                                      arg.lease ? lease_issuer_.get_lease() : 0)
                        .on_failure().add_range_request(path, true);
                    expected_existence.back().push_back(false);

                    if (auto parent = as_path_string_(arg.key.parent()); !parent.empty()) {
                        bldr.add_check_exists(parent)
                            .on_failure().add_range_request(parent, true);
                        expected_existence.back().push_back(true);
                    }

                    create_indices.push_back(_i++);
                } else if constexpr (std::is_same_v<T, TxnOpSet>) {
                    expected_existence.emplace_back();

                    bldr.add_check_exists(path)
                        .on_success().add_put_request(path, arg.value)
                                     .add_range_request(path)
                        .on_failure().add_range_request(path, true);
                    expected_existence.back().push_back(true);

                    set_indices.push_back((++_i)++);
                } else if constexpr (std::is_same_v<T, TxnOpErase>) {
                    expected_existence.emplace_back();

                    bldr.add_check_exists(path)
                        .on_success().add_delete_range_request(path)
                        .on_failure().add_range_request(path, true);
                    expected_existence.back().push_back(true);

                    ++_i;
                } else static_assert(detail::always_false<T>::value, "non-exhaustive visitor");
            }, op);
        }

        TxnResponse response = commit_(context, bldr.get_transaction());

        if (!response.succeeded()) {
            auto& responses = *response.mutable_responses();

            for (size_t i = 0; i < transaction.checks.size(); ++i) {
                auto resp_i = responses[i].release_response_range();
                if (resp_i->kvs_size() == 0 || transaction.checks[i].version != resp_i->kvs(0).version())
                    throw TxnFailed{i};
            }

            size_t tr_checks_number = transaction.checks.size(), j = 0;
            for (size_t i = 0; i < transaction.ops.size(); ++i)
                for (bool should_exist : expected_existence[i]) {
                    if (should_exist ^ static_cast<bool>(responses[j + tr_checks_number]
                                                                  .release_response_range()->kvs_size()))
                        throw TxnFailed{i + tr_checks_number};
                    ++j;
                }

            // not connected with logic
            throw TxnFailed{static_cast<size_t>(-1)};
        }

        TransactionResult result;

        int i = 0, j = 0;
        while (i < create_indices.size() && j < set_indices.size())
            if (create_indices[i] < set_indices[j]) {
                result.push_back(TxnOpResult{TxnOpResult::Kind::CREATE, 1});
                ++i;
            } else {
                result.push_back(TxnOpResult{TxnOpResult::Kind::SET, static_cast<int64_t>(
                                               response.mutable_responses(
                                                       set_indices[j])->release_response_range()
                                                   ->kvs(0).version())});
                ++j;
            }

        while (i++ < create_indices.size())
            result.push_back(TxnOpResult{TxnOpResult::Kind::CREATE, 1});

        while (j < set_indices.size())
            result.push_back(TxnOpResult{TxnOpResult::Kind::SET, static_cast<int64_t>(
                                                response.mutable_responses(
                                                        set_indices[j++])->release_response_range()
                                                    ->kvs(0).version())});
        return result;
    }
};

} // namespace liboffkv