#pragma once

#include <ppconsul/consul.h>
#include <ppconsul/kv.h>
#include <ppconsul/sessions.h>
#include <string>
#include <vector>
#include <utility>
#include <type_traits>
#include <stdint.h>

#include "client.hpp"
#include "key.hpp"
#include "util.hpp"

namespace liboffkv {

class ConsulClient : public Client
{
private:
    static constexpr auto WATCH_TIMEOUT = std::chrono::seconds(120);
    static constexpr auto TTL = std::chrono::seconds(10);
    static constexpr auto CONSISTENCY = ppconsul::Consistency::Consistent;

    ppconsul::Consul client_;
    ppconsul::kv::Kv kv_;
    std::string address_;
    std::string session_id_;

    [[noreturn]] static void rethrow_(const ppconsul::Error &e)
    {
        if (auto bad_status = dynamic_cast<const ppconsul::BadStatus *>(&e))
            throw ServiceError(bad_status->what());
        throw e;
    }

    std::string as_path_string_(const Path &path) const
    {
        std::string result;
        for (auto segment : (prefix_ / path).segments()) {
            if (!result.empty())
                result += '/';
            result.append(segment);
        }
        return result;
    }

    class ConsulWatchHandle_ : public WatchHandle
    {
        ppconsul::Consul client_;
        ppconsul::kv::Kv kv_;
        std::string key_;
        uint64_t old_version_;
        bool all_with_prefix_;

    public:
        ConsulWatchHandle_(
                    const std::string &address,
                    std::string key,
                    uint64_t old_version,
                    bool all_with_prefix)
            : client_(address)
            , kv_(client_, ppconsul::kw::consistency = CONSISTENCY)
            , key_(std::move(key))
            , old_version_{old_version}
            , all_with_prefix_{all_with_prefix}
        {}

        void wait() override
        {
            if (all_with_prefix_)
                kv_.keys(key_, ppconsul::kv::kw::block_for = {WATCH_TIMEOUT, old_version_});
            else
                kv_.item(key_, ppconsul::kv::kw::block_for = {WATCH_TIMEOUT, old_version_});
        }
    };

    std::unique_ptr<WatchHandle> make_watch_handle_(
        const std::string &key,
        uint64_t old_version,
        bool all_with_prefix = false) const
    {
        return std::make_unique<ConsulWatchHandle_>(address_, key, old_version, all_with_prefix);
    }

public:
    ConsulClient(const std::string &address, Path prefix)
        : Client(std::move(prefix))
        , client_(address)
        , kv_(client_, ppconsul::kw::consistency = CONSISTENCY)
        , address_{address}
        , session_id_{}
    {}

    int64_t create(const Key &key, const std::string &value, bool lease = false) override
    {
        if (lease)
            throw "TODO";

        std::vector<ppconsul::kv::TxnOperation> txn;

        const Path parent = key.parent();
        const bool has_parent = !parent.root();

        if (has_parent)
            txn.push_back(ppconsul::kv::txn_ops::Get{as_path_string_(parent)});

        const std::string key_string = as_path_string_(key);
        txn.push_back(ppconsul::kv::txn_ops::CheckNotExists{key_string});
        txn.push_back(ppconsul::kv::txn_ops::Set{key_string, value});

        try {
            return kv_.commit(txn).back().modifyIndex;

        } catch (const ppconsul::kv::TxnAborted &e) {
            const auto op_index = e.errors().front().opIndex;
            if (has_parent && op_index == 0)
                throw NoEntry{};
            if (op_index == (has_parent ? 1 : 0))
                throw EntryExists{};
            throw;

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    ExistsResult exists(const Key &key, bool watch = false) override
    {
        const std::string key_string = as_path_string_(key);
        try {
            auto item = kv_.item(ppconsul::withHeaders, key_string);
            std::unique_ptr<WatchHandle> watch_handle;
            const int64_t version =
                item.data().valid() ? static_cast<int64_t>(item.headers().index()) : 0;
            if (watch)
                watch_handle = make_watch_handle_(key_string, version);
            return {version, std::move(watch_handle)};

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    ChildrenResult get_children(const Key &key, bool watch = false) override
    {
        const std::string key_string = as_path_string_(key);
        const std::string child_prefix = key_string + "/";
        try {
            auto result = kv_.commit({
                ppconsul::kv::txn_ops::GetAll{child_prefix},
                ppconsul::kv::txn_ops::Get{key_string},
            });

            std::unique_ptr<WatchHandle> watch_handle;
            if (watch)
                watch_handle = make_watch_handle_(child_prefix, result.back().modifyIndex, true);

            std::vector<std::string> children;
            const auto nchild_prefix = child_prefix.size();
            const auto nglobal_prefix = as_path_string_(prefix_).size();
            for (auto it = result.begin(), end = --result.end(); it != end; ++it)
                if (it->key.find('/', nchild_prefix) == std::string::npos) {
                    if (prefix_.root())
                        children.emplace_back("/" + it->key);
                    else
                        children.emplace_back(it->key.substr(nglobal_prefix));
                }

            return {std::move(children), std::move(watch_handle)};

        } catch (const ppconsul::kv::TxnAborted &) {
            throw NoEntry{};

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    int64_t set(const Key &key, const std::string &value) override
    {
        const Path parent = key.parent();

        std::vector<ppconsul::kv::TxnOperation> txn;
        if (!parent.root())
            txn.push_back(ppconsul::kv::txn_ops::Get{as_path_string_(parent)});
        txn.push_back(ppconsul::kv::txn_ops::Set{as_path_string_(key), value});

        try {
            auto result = kv_.commit(txn);
            return result.back().modifyIndex;

        } catch (const ppconsul::kv::TxnAborted &) {
            throw NoEntry{};

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    GetResult get(const Key &key, bool watch = false) override
    {
        const std::string key_string = as_path_string_(key);

        try {
            auto item = kv_.item(ppconsul::withHeaders, key_string);
            if (!item.data().valid())
                throw NoEntry{};
            const int64_t version = static_cast<int64_t>(item.headers().index());

            std::unique_ptr<WatchHandle> watch_handle;
            if (watch)
                watch_handle = make_watch_handle_(key_string, version);

            return {version, item.data().value, std::move(watch_handle)};

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    CasResult cas(const Key &key, const std::string &value, int64_t version = 0) override
    {
        if (!version)
            try {
                return {create(key, value)};
            } catch (const EntryExists &) {
                return {0};
            }

        const std::string key_string = as_path_string_(key);
        try {
            auto result = kv_.commit({
                ppconsul::kv::txn_ops::Get{key_string},
                ppconsul::kv::txn_ops::CompareSet{key_string, static_cast<uint64_t>(version), value},
            });
            return {static_cast<int64_t>(result.back().modifyIndex)};

        } catch (const ppconsul::kv::TxnAborted &e) {
            const auto op_index = e.errors().front().opIndex;
            if (op_index == 0)
                throw NoEntry{};
            return {0};

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    void erase(const Key &key, int64_t version = 0) override
    {
        const std::string key_string = as_path_string_(key);

        std::vector<ppconsul::kv::TxnOperation> txn{
            ppconsul::kv::txn_ops::Get{key_string}
        };
        if (version) {
            txn.push_back(ppconsul::kv::txn_ops::CompareErase{
                key_string,
                static_cast<uint64_t>(version)
            });
        } else {
            txn.push_back(ppconsul::kv::txn_ops::Erase{key_string});
            txn.push_back(ppconsul::kv::txn_ops::EraseAll{key_string + "/"});
        }

        try {
            kv_.commit(txn);

        } catch (const ppconsul::kv::TxnAborted &e) {
            const auto op_index = e.errors().front().opIndex;
            if (op_index == 0)
                throw NoEntry{};
            throw;

        } catch (const ppconsul::Error &e) {
            rethrow_(e);
        }
    }

    std::vector<TxnOpResult> commit(
        const std::vector<TxnCheck> &checks,
        const std::vector<TxnOp> &ops) override
    {
        enum class ResultKind
        {
            CREATE,
            SET,
            AUX,
        };

        std::vector<ppconsul::kv::TxnOperation> txn;
        std::vector<size_t> boundaries;
        std::vector<ResultKind> result_kinds;

        for (const auto &check : checks) {
            if (check.version) {
                txn.push_back(ppconsul::kv::txn_ops::CheckIndex{
                    as_path_string_(check.key),
                    static_cast<uint64_t>(check.version)
                });
            } else {
                txn.push_back(ppconsul::kv::txn_ops::Get{
                    as_path_string_(check.key)
                });
            }
            result_kinds.push_back(ResultKind::AUX);
            boundaries.push_back(txn.size() - 1);
        }

        for (const auto &op : ops) {
            std::visit([this, &txn, &result_kinds](auto &&arg) {
                using T = std::decay_t<decltype(arg)>;
                const std::string key_string = as_path_string_(arg.key);

                if constexpr (std::is_same_v<T, TxnOpCreate>) {

                    const Path parent = arg.key.parent();
                    if (!parent.root()) {
                        txn.push_back(ppconsul::kv::txn_ops::Get{as_path_string_(parent)});
                        result_kinds.push_back(ResultKind::AUX);
                    }

                    txn.push_back(ppconsul::kv::txn_ops::CheckNotExists{key_string});
                    // CheckNotExists does not produce any results

                    txn.push_back(ppconsul::kv::txn_ops::Set{key_string, arg.value});
                    result_kinds.push_back(ResultKind::CREATE);

                } else if constexpr (std::is_same_v<T, TxnOpSet>) {

                    txn.push_back(ppconsul::kv::txn_ops::Get{key_string});
                    result_kinds.push_back(ResultKind::AUX);

                    txn.push_back(ppconsul::kv::txn_ops::Set{key_string, arg.value});
                    result_kinds.push_back(ResultKind::SET);

                } else if constexpr (std::is_same_v<T, TxnOpErase>) {

                    txn.push_back(ppconsul::kv::txn_ops::Get{key_string});
                    result_kinds.push_back(ResultKind::AUX);

                    txn.push_back(ppconsul::kv::txn_ops::Erase{key_string});
                    // Erase does not produce any results

                    txn.push_back(ppconsul::kv::txn_ops::EraseAll{key_string + "/"});
                    // EraseAll does not produce any results

                } else
                    static_assert(util::always_false<T>::value, "non-exhaustive visitor");
            }, op);

            boundaries.push_back(txn.size() - 1);
        }

        try {
            auto results = kv_.commit(txn);
            std::vector<TxnOpResult> answer;
            for (size_t i = 0; i < results.size(); ++i) {
                switch (result_kinds[i]) {
                case ResultKind::CREATE:
                    answer.push_back(TxnCreateResult{static_cast<int64_t>(results[i].modifyIndex)});
                    break;
                case ResultKind::SET:
                    answer.push_back(TxnSetResult{static_cast<int64_t>(results[i].modifyIndex)});
                    break;
                case ResultKind::AUX:
                    break;
                }
            }
            return answer;

        } catch (const ppconsul::kv::TxnAborted &e) {
            const auto op_index = e.errors().front().opIndex;
            const size_t user_op_index =
                std::lower_bound(boundaries.begin(), boundaries.end(), op_index)
                - boundaries.begin();
            throw TxnFailed{user_op_index};
        }
    }
};

} // namespace liboffkv
