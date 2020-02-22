#pragma once

#include <grpcpp/grpcpp.h>
#include <libetcd/kv.pb.h>
#include <libetcd/rpc.pb.h>
#include <libetcd/rpc.grpc.pb.h>
#include <grpcpp/security/credentials.h>

#include <future>
#include <mutex>


#include "key.hpp"
#include "ping_sender.hpp"


namespace liboffkv {

namespace detail {

void ensure_succeeded_(grpc::Status status)
{
    if (status.ok()) return;

    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        throw ConnectionLoss{};
    }
    throw ServiceError{status.error_message()};
}

} // namespace detail


class ETCDWatchCreator {
public:
    using WatchCreateRequest = etcdserverpb::WatchCreateRequest;
    using WatchResponse = etcdserverpb::WatchResponse;
    using Event = mvccpb::Event;
    using EventType = mvccpb::Event_EventType;

    struct WatchEventHandler {
        std::function<bool(const Event&)> process_event;
        std::function<void(const ServiceError&)> process_failure;
    };

private:
    static inline void* const tag_init_stream = reinterpret_cast<void*>(1);
    static inline void* const tag_write_finished = reinterpret_cast<void*>(2);
    static inline void* const tag_response_got = reinterpret_cast<void*>(3);

    using WatchRequest = etcdserverpb::WatchRequest;
    using WatchCancelRequest = etcdserverpb::WatchCancelRequest;
    using WatchEndpoint = etcdserverpb::Watch;

    std::mutex lock_;

    std::unique_ptr<WatchEndpoint::Stub> watch_stub_;
    grpc::ClientContext watch_context_;
    std::shared_ptr<grpc::ClientAsyncReaderWriter<WatchRequest, WatchResponse>> watch_stream_;
    std::unique_ptr<WatchResponse> pending_watch_response_;
    std::map<int64_t, WatchEventHandler> watch_handlers_;

    grpc::CompletionQueue cq_;

    bool watch_resolution_thread_running_ = false;
    std::thread watch_resolution_thread_;

    std::unique_ptr<std::promise<void>> current_watch_write_;
    std::unique_ptr<WatchEventHandler> pending_watch_handler_;

    std::condition_variable write_wait_cv_;
    std::condition_variable create_watch_wait_cv_;


    void fail_all_watches(const ServiceError& exc)
    {
        std::lock_guard lock(lock_);

        watch_stream_ = nullptr;
        pending_watch_response_ = nullptr;

        for (const auto& [_, watch_handler] : watch_handlers_) (void)_, watch_handler.process_failure(exc);

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
        if (watch_stream_) return;
        if (current_watch_write_) throw std::logic_error("Inconsistent internal state");

        // just to slow down next write until stream init
        current_watch_write_ = std::make_unique<std::promise<void>>();
        watch_stream_ = watch_stub_->AsyncWatch(&watch_context_, &cq_, tag_init_stream);

        if (!watch_resolution_thread_running_) {
            watch_resolution_thread_running_ = true;
            watch_resolution_thread_ = std::thread([this] { this->watch_resolution_loop_(); });
        }
    }

    void resolve_write_m()
    {
        current_watch_write_->set_value();
        current_watch_write_ = nullptr;
        write_wait_cv_.notify_one();
    }

    void watch_resolution_loop_()
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
                if (auto* response = pending_watch_response_.release()) {
                    if (response->created()) {
                        watch_handlers_[response->watch_id()] = *pending_watch_handler_;
                        pending_watch_handler_ = nullptr;
                        create_watch_wait_cv_.notify_one();
                    }

                    if (!(response->created() || response->canceled()) && process_watch_response_m(*response)) {
                        cancel_watch_m(response->watch_id(), lock);
                    }

                    delete response;
                }

                request_read_next_watch_response_m();
            }
        }
    }

    void cancel_watch_m(int64_t watch_id, std::unique_lock<std::mutex>& lock)
    {
        auto* cancel_req = new WatchCancelRequest();
        cancel_req->set_watch_id(watch_id);
        WatchRequest req;
        req.set_allocated_cancel_request(cancel_req);
        write_to_watch_stream_m(req, lock);
    }

    bool process_watch_response_m(const WatchResponse& response)
    {
        if (!watch_handlers_.count(response.watch_id())) return true;
        WatchEventHandler& handler = watch_handlers_[response.watch_id()];

        for (const Event& event : response.events()) {
            if (handler.process_event(event)) {
                watch_handlers_.erase(response.watch_id());
                return true;
            }
        }
        return false;
    }

    void request_read_next_watch_response_m()
    {
        if (pending_watch_response_) throw std::logic_error("Inconsistent internal state");

        pending_watch_response_ = std::make_unique<WatchResponse>();
        watch_stream_->Read(pending_watch_response_.get(), tag_response_got);
    }

    std::future<void> write_to_watch_stream_m(const WatchRequest& request, std::unique_lock<std::mutex>& lock)
    {
        setup_watch_infrastructure_m();

        while (current_watch_write_) write_wait_cv_.wait(lock);

        current_watch_write_ = std::make_unique<std::promise<void>>();
        watch_stream_->Write(request, tag_write_finished);

        return current_watch_write_->get_future();
    }


public:
    ETCDWatchCreator(const std::shared_ptr<grpc::Channel>& channel)
        : watch_stub_(WatchEndpoint::NewStub(channel))
    {}

    void create_watch(const WatchCreateRequest& create_req, const WatchEventHandler& handler)
    {
        WatchRequest request;
        request.set_allocated_create_request(new WatchCreateRequest(create_req));

        std::future<void> watch_write_future;
        {
            std::unique_lock lock(lock_);
            while (pending_watch_handler_) create_watch_wait_cv_.wait(lock);

            pending_watch_handler_ = std::make_unique<WatchEventHandler>(handler);
            watch_write_future = write_to_watch_stream_m(request, lock);
        }

        watch_write_future.get();
    }

    ~ETCDWatchCreator()
    {
        bool do_join;
        {
            std::unique_lock lock(lock_);
            if (watch_stream_) watch_context_.TryCancel();
            cq_.Shutdown();
            do_join = watch_resolution_thread_running_;
        }
        if (do_join) watch_resolution_thread_.join();
    }
};


class LeaseIssuer {
private:
    static inline const size_t LEASE_TIMEOUT = 10; // seconds

    using LeaseEndpoint = etcdserverpb::Lease;

    int64_t lease_id_{0};
    std::unique_ptr<LeaseEndpoint::Stub> lease_stub_;
    detail::PingSender ping_sender_;


    void setup_lease_renewal_(int64_t lease_id, std::chrono::seconds ttl)
    {
        auto context_ptr = std::make_shared<grpc::ClientContext>();
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

        detail::ensure_succeeded_(status);

        setup_lease_renewal_(response.id(), std::chrono::seconds(response.ttl()));
        return response.id();
    }


public:
    LeaseIssuer(const std::shared_ptr<grpc::Channel>& channel)
        : lease_stub_(LeaseEndpoint::NewStub(channel))
    {}

    int64_t get_lease()
    {
        if (!lease_id_) lease_id_ = create_lease_();
        return lease_id_;
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
        std::visit([request](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;

            if constexpr (std::is_same_v<T, std::string>) {
                if (arg.size()) request->set_key(arg);
            } else if constexpr (std::is_same_v<T, std::pair<std::string, std::string>>) {
                auto [begin, end] = arg;
                request->set_key(begin);
                request->set_range_end(end);
            } else static_assert(detail::always_false<T>::value, "non-exhaustive visitor");
        }, range);
    }

    enum class TBStatus {
        UNDEFINED,
        SUCCESS,
        FAILURE,
    } status_;

public:
    TxnRequest& get_transaction()
    {
        return txn_;
    }

    ETCDTransactionBuilder& add_check_exists(const std::string& key)
    {
        auto cmp = make_compare_(key, Compare::CREATE, Compare::GREATER);
        cmp->set_create_revision(0);

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
        status_ = TBStatus::SUCCESS;
        return *this;
    }

    ETCDTransactionBuilder& on_failure()
    {
        status_ = TBStatus::FAILURE;
        return *this;
    }

    ETCDTransactionBuilder& add_put_request(const std::string& key, const std::string& value,
                                            int64_t lease = 0, bool ignore_lease = false)
    {
        assert(status_ != TBStatus::UNDEFINED);
        auto request = new PutRequest();
        request->set_key(key);
        request->set_value(value);

        if (ignore_lease) request->set_ignore_lease(true);
        else              request->set_lease(lease);

        auto requestOp = status_ == TBStatus::SUCCESS
            ? txn_.add_success()
            : txn_.add_failure();
        requestOp->set_allocated_request_put(request);

        return *this;
    }

    ETCDTransactionBuilder& add_range_request(
        const std::variant<std::string, std::pair<std::string, std::string>>& range,
        bool keys_only = false, int64_t limit = 1)
    {
        assert(status_ != TBStatus::UNDEFINED);

        auto request = new RangeRequest();
        set_key_range_(request, range);
        request->set_limit(limit);
        request->set_keys_only(keys_only);

        auto requestOp = status_ == TBStatus::SUCCESS
            ? txn_.add_success()
            : txn_.add_failure();
        requestOp->set_allocated_request_range(request);

        return *this;
    }

    ETCDTransactionBuilder& add_delete_range_request(
        const std::variant<std::string, std::pair<std::string, std::string>>& range)
    {
        assert(status_ != TBStatus::UNDEFINED);

        auto request = new DeleteRangeRequest();
        set_key_range_(request, range);

        auto requestOp = status_ == TBStatus::SUCCESS
            ? txn_.add_success()
            : txn_.add_failure();
        requestOp->set_allocated_request_delete_range(request);

        return *this;
    }
};



class ETCDClient : public Client {
private:
    using KV = etcdserverpb::KV;

    using RangeRequest = etcdserverpb::RangeRequest;
    using RangeResponse = etcdserverpb::RangeResponse;
    using PutRequest = etcdserverpb::PutRequest;
    using PutResponse = etcdserverpb::PutResponse;
    using TxnRequest = etcdserverpb::TxnRequest;
    using TxnResponse = etcdserverpb::TxnResponse;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<KV::Stub> stub_;

    ETCDWatchCreator watch_creator_;
    LeaseIssuer lease_issuer_;


    // to simplify further search of direct children and subtree range
    // each path will be transformed in the following way:
    // before the __last__ segment special symbol '\0' is inserted:
    // as_path_string_("/prefix/a/b") -> "/prefix/a/\0b"
    std::string as_path_string_(const Path &path) const
    {
        std::string result;
        result.reserve(path.size() + prefix_.size() + 2);
        result.append(static_cast<std::string>(prefix_));

        if (path.root()) return result;

        auto segments = path.segments();
        for (size_t i = 0; i < segments.size() - 1; ++i) {
            result.append("/").append(segments[i]);
        }

        return result.append("/\0", 2).append(segments.back());
    }

    // key lies in the subtree of <root> iff it has form "{root}/something"
    auto make_subtree_range_(const Path& root)
    {
        auto path = static_cast<std::string>(prefix_ / root);

        // any sequence beginning with '/'
        return std::make_pair(
            path + '/',
            path + static_cast<char>('/' + 1)
        );
    }

    // "{root}/child" is a direct child of <root> iff the is no '/' in "{child}"
    // or in forestated terminology, {child} is the last segment
    // that means "{child}" begins with '\0'
    auto make_direct_children_range_(const Key& root)
    {
        auto path = static_cast<std::string>(prefix_ / root);

        // any sequence beginning with "/\0"
        return std::make_pair(
            path + '/' + '\0',
            path + '/' + static_cast<char>('\0' + 1)
        );
    }

    // removes prefix and auxiliary \0
    std::string unwrap_key_(const std::string& full_path) const
    {
        size_t pos = full_path.rfind('/');
        return full_path.substr(prefix_.size(), pos + 1 - prefix_.size()) + full_path.substr(pos + 2);
    }

    TxnResponse commit_(grpc::ClientContext& context, const TxnRequest& txn)
    {
        TxnResponse response;

        auto status = stub_->Txn(&context, txn, &response);
        detail::ensure_succeeded_(status);

        return response;
    }


    class ETCDWatchHandle_ : public WatchHandle {
    private:
        std::shared_future<void> future_;

    public:
        ETCDWatchHandle_(std::shared_future<void>&& future)
            : future_(std::move(future))
        {}

        void wait() override { future_.get(); }
    };


    template <typename EventChecker>
    std::unique_ptr<WatchHandle> make_watch_handle_(
            const ETCDWatchCreator::WatchCreateRequest& request,
            EventChecker&& stop_waiting_condition
        )
    {
        auto promise = std::make_shared<std::promise<void>>();
        watch_creator_.create_watch(
            request,
            {
                [promise, foo = std::forward<EventChecker>(stop_waiting_condition)]
                    (const ETCDWatchCreator::Event& event) mutable
                {
                    if (foo(event)) {
                        promise->set_value();
                        return true;
                    }
                    return false;
                },
                [promise](const ServiceError& exc)
                {
                    promise->set_exception(std::make_exception_ptr(exc));
                }
            });
        return std::make_unique<ETCDWatchHandle_>(promise->get_future().share());
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

        if (auto parent = key.parent(); !parent.root()) bldr.add_check_exists(as_path_string_(parent));

        bldr.add_check_not_exists(path)
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
        detail::ensure_succeeded_(status);

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

        auto [key_begin, key_end] = make_direct_children_range_(key);

        ETCDTransactionBuilder bldr;
        bldr.add_check_exists(as_path_string_(key))
            .on_success().add_range_request(std::make_pair(key_begin, key_end), true, 0);

        TxnResponse response = commit_(context, bldr.get_transaction());
        if (!response.succeeded()) throw NoEntry{};

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

            if (auto parent = key.parent(); !parent.root()) {
                auto path = as_path_string_(parent);
                bldr.add_check_exists(path)
                    .on_failure().add_range_request(path, true);
            }

            auto path = as_path_string_(key);

            if (!expect_lease) bldr.add_lease_compare(path, 0);

            bldr.on_success().add_put_request(path, value, 0, expect_lease)
                             .add_range_request(path);

            TxnResponse response;
            auto status = stub_->Txn(&context, bldr.get_transaction(), &response);
            if (status.error_code() == grpc::StatusCode::INVALID_ARGUMENT) {
                continue;
            }
            detail::ensure_succeeded_(status);

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
            if (failure_response->kvs_size()) return {0};

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

        detail::ensure_succeeded_(status);

        if (!response.kvs_size()) throw NoEntry{};

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
                         .add_delete_range_request(make_subtree_range_(key))
            .on_failure().add_range_request(path, true);

        TxnResponse response = commit_(context, bldr.get_transaction());
        if (!response.succeeded() && !response.mutable_responses(0)->release_response_range()->count()) {
            throw NoEntry{};
        }
    }


    TransactionResult commit(const Transaction& transaction) override
    {
        grpc::ClientContext context;

        std::vector<size_t> set_indices, create_indices;
        std::vector<std::vector<bool>> expected_existence;
        size_t total_op_number = 0;

        ETCDTransactionBuilder bldr;

        for (const auto& check : transaction.checks) {
            auto path = as_path_string_(check.key);
            bldr.add_version_compare(path, check.version)
                .on_failure().add_range_request(path);
        }

        for (const auto& op : transaction.ops) {
            std::visit([this, &expected_existence,
                        &create_indices, &set_indices,
                        &total_op_number, &bldr](auto&& arg) {
                auto path = as_path_string_(arg.key);
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, TxnOpCreate>) {
                    expected_existence.emplace_back();
                    bldr.add_check_not_exists(path)
                        .on_success().add_put_request(path, arg.value,
                                                      arg.lease ? lease_issuer_.get_lease() : 0)
                        .on_failure().add_range_request(path, true);
                    expected_existence.back().push_back(false);

                    if (auto parent = arg.key.parent(); !parent.root()) {
                        auto path = as_path_string_(parent);
                        bldr.add_check_exists(path)
                            .on_failure().add_range_request(path, true);
                        expected_existence.back().push_back(true);
                    }

                    create_indices.push_back(total_op_number++);
                } else if constexpr (std::is_same_v<T, TxnOpSet>) {
                    expected_existence.emplace_back();

                    bldr.add_check_exists(path)
                        .on_success().add_put_request(path, arg.value)
                                     .add_range_request(path)
                        .on_failure().add_range_request(path, true);
                    expected_existence.back().push_back(true);

                    // skip put request
                    ++total_op_number;

                    // save range request index
                    set_indices.push_back(total_op_number++);
                } else if constexpr (std::is_same_v<T, TxnOpErase>) {
                    expected_existence.emplace_back();

                    bldr.add_check_exists(path)
                        .on_success().add_delete_range_request(path)
                                     .add_delete_range_request(make_subtree_range_(arg.key))
                        .on_failure().add_range_request(path, true);
                    expected_existence.back().push_back(true);

                    ++total_op_number;
                } else static_assert(detail::always_false<T>::value, "non-exhaustive visitor");
            }, op);
        }

        TxnResponse response = commit_(context, bldr.get_transaction());

        if (!response.succeeded()) {
            auto& responses = *response.mutable_responses();

            // check if any check failed
            for (size_t i = 0; i < transaction.checks.size(); ++i) {
                auto resp_i = responses[i].release_response_range();
                if (resp_i->kvs_size() == 0 || transaction.checks[i].version != resp_i->kvs(0).version()) {
                    throw TxnFailed{i};
                }
            }

            size_t tr_checks_number = transaction.checks.size(), j = 0;
            // check if any op failed
            for (size_t i = 0; i < transaction.ops.size(); ++i) {
                for (bool should_exist : expected_existence[i]) {
                    if (should_exist ^ static_cast<bool>(responses[j + tr_checks_number]
                                                            .release_response_range()
                                                            ->kvs_size()))
                        throw TxnFailed{i + tr_checks_number};
                    ++j;
                }
            }

            throw ServiceError{"we are sorry for your transaction"};
        }

        TransactionResult result;

        size_t i = 0, j = 0;
        while (i != create_indices.size() || j != set_indices.size()) {
            if (j == set_indices.size()
                || (i != create_indices.size() && create_indices[i] < set_indices[j])) {
                result.push_back(TxnOpResult{TxnOpResult::Kind::CREATE, 1});
                ++i;
            } else {
                result.push_back(TxnOpResult{
                    TxnOpResult::Kind::SET,
                    static_cast<int64_t>(
                        response.mutable_responses(set_indices[j])
                                ->release_response_range()
                                ->kvs(0)
                                .version())
                });
                ++j;
            }
        }
        return result;
    }
};

} // namespace liboffkv
