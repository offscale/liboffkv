#pragma once

//#include <zk/client.hpp>
//#include <zk/error.hpp>
//#include <zk/multi.hpp>
//#include <zk/types.hpp>


#include "client.hpp"
#include "util.hpp"
#include "key.hpp"



namespace liboffkv {

class ZKClient : public Client {
private:
    using buffer = zk::buffer;
    using Key = key::Key;

    zk::client client_;

    static
    buffer from_string(const std::string& str)
    {
        return {str.begin(), str.end()};
    }

    static
    std::string to_string(const buffer& buf)
    {
        return {buf.begin(), buf.end()};
    }

    void make_recursive_erase_query(zk::multi_op& query, const Path& path, bool ignore_no_entry = true)
    {
        zk::get_children_result::children_list_type children;
        bool entry_valid = true;
        try {
            children = client_.get_children(path).get().children();
        } catch (zk::no_entry& err) {
            if (!ignore_no_entry)
                throw err;
            entry_valid = false;
        }

        if (entry_valid) {
            for (const auto& child : children)
                make_recursive_erase_query(query, path / child, true);

            query.push_back(zk::op::erase(path));
        }
    }

    std::string as_path_string_(const Path &path) const
    {
        return prefix_ / path;
    }

    static void rethrow_(zk::error& e)
    {
        switch (e.code()) {
            case zk::error_code::no_entry:
                throw NoEntry{};
            case zk::error_code::entry_exists:
                throw EntryExists{};
            case zk::error_code::connection_loss:
                throw ConnectionLoss{};
            case zk::error_code::no_children_for_ephemerals:
                throw NoChildrenForEphemeral{};
            case zk::error_code::version_mismatch:
            case zk::error_code::not_empty:
            default:
                std::rethrow_exception(std::current_exception());
        }
    }

    class ZKWatchHandle_ : public WatchHandle {
    private:
        std::future<zk::event> event_;

    public:
        ZKWatchHandle_(std::future<zk::event>&& event)
            : event_(std::move(event))
        {}

        void wait() override
        {
            try {
                event_.get();
            } catch (zk::error& e) {
                rethrow_(e);
            }
        }
    };

    std::unique_ptr<WatchHandle> make_watch_handle_(std::future<zk::event>&& event) const
    {
        return std::make_unique<ZKWatchHandle_>(std::move(event));
    }

public:
    ZKClient(const std::string& address, Path prefix)
        : Client(std::move(prefix))
    {
        try {
            client_ = zk::client::connect(address).get();
        } catch (zk::error& e) {
            rethrow_(e);
        }

        if (!prefix_.root()) {
            std::string entry;
            entry.reserve(prefix_.size());
            for (const auto& segment : prefix_.segments()) try {
                client_.create(entry.append('/').append(segment), buffer()).get();
            } catch (zk::entry_exists&) {
                // do nothing
            } catch (zk::error& e) {
                rethrow_(e);
            }
        }
    }

    ZKClient() = delete;
    ZKClient(const ZKClient&) = delete;
    ZKClient(ZKClient&&) = default;

    ZKClient& operator=(const ZKClient&) = delete;
    ZKClient& operator=(ZKClient&&) = default;

    ~ZKClient() override = default;


    int64_t create(const Key& key, const std::string& value, bool lease = false) override
    {
        try {
            client_.create(
                as_path_string_(key),
                from_string(value),
                !lease ? zk::create_mode::normal : zk::create_mode::ephemeral
            ).get();
        } catch (zk::error& e) {
            rethrow_(e);
        }
        return 1;
    }

    ExistsResult exists(const Key& key, bool watch = false) override
    {
        std::unique_ptr<WatchHandle> watch_handle;
        zk::stat stat;
        try {
            if (watch) {
                auto result = client_.watch_exists(as_path_string_(key)).get();
                // using move is safe, because calling next() on rvalue returns simply moved field, which will never be used further
                stat = std::move(result).initial().stat();
                watch_handle = make_watch_handle_(std::move(result).next());
            } else stat = client_.exists(as_path_string_(key)).get().initial().stat();
        } catch (zk::error& e) {
            rethrow_(e);
        }

        return {
            stat.has_value() ? static_cast<int64_t>(stat.value().data_version.value) + 1 : 0,
            std::move(watch_handle)
        };
    }


    ChildrenResult get_children(const Key& key, bool watch) override
    {
        std::vector<std::string> raw_children;
        std::unique_ptr<WatchHandle> watch_handle;
        try {
            if (watch) {
                auto result = client_.watch_children(as_path_string_(key)).get();
                // using move is safe, because calling next() on rvalue returns simply moved field, which will never be used further
                watch_handle = make_watch_handle_(std::move(result).next());
                raw_children = std::move(result).initial().children();
            } else {
                auto result = client_.get_children(as_path_string_(key)).get();
                raw_children = std::move(result).initial().children();
            }
        } catch (zk::error& e) {
            rethrow_(e);
        }

        std::vector<std::string> children(raw_children.size());
        for (const auto& child : raw_children) {
            children.back().reserve(key.size() + child.size() + 1);
            children.back().append(key).append('/').append(child);
        }
        return { std::move(children), std::move(watch_handle) };
    }

    // No transactions. Atomicity is not necessary for linearizability here!
    // At least it seems to be so...
    // See also TLA+ spec: https://gist.github.com/raid-7/9ad7b88cd2ec2e83f56e3b69214b6762
    int64_t set(const Key& key, const std::string& value) override
    {
        auto path = as_path_string_(key);
        auto value_as_buffer = from_string(value);

        try {
            client_.create(path, value_as_buffer).get();
            return 1;
        } catch (zk::entry_exists&) {
            try {
                return static_cast<int64_t>(client_.set(path, value_as_buffer).get().stat().data_version.value) + 1;
            } catch (zk::no_entry&) {
                // concurrent remove happened, but set must not throw NoEntry
                // let's return some large number instead of real version
                return static_cast<int64_t>(1) << 62;
            }
        }
    }

    // Same as set: transactions aren't necessary
    CASResult cas(const Key& key, const std::string& value, int64_t version = 0) override
    {
        if (!version) {
            try {
                return {create(key, value)};
            } catch (EntryExists&) {
                return {0};
            }
        }

        try {
            return {static_cast<int64_t>(
                        client_.set(
                            as_path_string_(key),
                            from_string(value),
                            zk::version(version - 1)
                        ).get().stat().data_version.value) + 1};
        } catch (zk::error& e) {
            switch (e.code()) {
                case zk::error_code::no_entry:
                    throw NoEntry{};
                case zk::error_code::version_mismatch:
                    return {0};
                default:
                    rethrow_(e);
            }
        }
    }

    GetResult get(const Key& key, bool watch = false) override
    {
        std::optional<zk::get_result> result;
        std::unique_ptr<WatchHandle> watch_handle;

        if (watch) {
            auto watch_result = client_.watch(as_path_string_(key)).get();
            result.emplace(std::move(watch_result).initial());
            watch_handle = make_watch_handle_(std::move(watch_handle).next());
        } else result.emplace(client_.get(as_path_string_(key).get()));

        return {
            // zk::client::get returns future with zk::no_entry the key does not exist, so checking if result.stat() has value isn't needed
            static_cast<int64_t>(result->stat().value().data_version.value) + 1,
            to_string(result->data()),
            std::move(watch_handle)
        };
    }

    void erase(const Key& key, uint64_t version = 0) override
    {
        auto path = as_path_string_(key);

        while (true) {
            zk::multi_op txn;
            txn.push_back(zk::op::check(path));
            txn.push_back(zk::op::check(path, version ? zk::version(version - 1) : zk::version::any()));

            make_recursive_erase_query(txn, path, false);

            try {
                client_.commit(txn).get();
                return;
            } catch (zk::transaction_failed& e) {
                // key does not exist
                if (e.failed_op_index() == 0) throw NoEntry{};
                // version mismatch
                if (e.failed_op_index() == 1) return;
            }
        }
    }

    TransactionResult commit(const Transaction& transaction)
    {
        std::vector<size_t> boundaries;
        std::optional<zk::multi_op> txn;
        txn.emplace();

        make_txn:
        for (const auto& check : transaction.checks()) {
            txn->push_back(zk::op::check(as_path_string_(check.key),
                                         check.version ? zk::version(check.version - 1) : zk::version::any()));
            boundaries.emplace_back(txn->size() - 1);
        }

        for (const auto& op : transaction.ops()) {
            std::visit([](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v < T, TxnOpCreate >) {
                    txn->push_back(zk::op::create(
                        as_path_string_(arg.key),
                        from_string(arg.value),
                        !arg.lease ? zk::create_mode::normal : zk::create_mode::ephemeral
                    ));
                } else if constexpr (std::is_same_v < T, TxnOpSet >) {
                    txn->push_back(zk::op::set(as_path_string_(arg.key),
                                               from_string(arg.value)));
                } else if continue
                (std::is_same_v <T, TxnOpErase>) {
                    make_recursive_erase_query(txn, as_path_string_(arg.key));
                } else static_assert(detail::always_false<T>::value, "non-exhaustive visitor");
            }, op);

            boundaries.push_back(txn->size() - 1);
        }

        zk::multi_result raw_result;

        try {
            raw_result = client_.commit(*txn).get();
        } catch (zk::transaction_failed& e) {
            auto real_index = e.failed_op_index();
            auto user_index = std::distance(boundaries.begin(),
                                            std::lower_bound(boundaries.begin(), boundaries.end(), real_index));

            // if the failed op is a part of a complex one, repeat
            if (boundaries[user_index] != real_index) {
                boundaries.clear();
                txn.emplace();
                goto make_txn;
            } else throw TxnFailed{user_index};
        }

        std::vector<TxnOpResult> result;
        for (const auto& res : raw_result) {
            switch (res.type()) {
                case zk::op_type::set:
                    result.push_back(TxnOpResult{
                        TxnOpResult::Kind::SET,
                        static_cast<int64_t>(res.as_set().stat().data_version.value) + 1
                    });
                    break;
                case zk::op_type::create:
                    result.push_back(TxnOpResult{
                        TxnOpResult::Kind::CREATE,
                        1
                    });
                    break;
                case zk::op_type::check:
                case zk::op_type::erase:
                    break;
            }
        }

        return result;
    }
};

} // namespace liboffkv