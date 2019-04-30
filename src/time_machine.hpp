#pragma once

#include <exception>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <utility>
#include <deque>
#include <atomic>


namespace time_machine {

    class QueueClosed : public std::runtime_error {
    public:
        QueueClosed() : std::runtime_error("Queue closed for Puts") {
        }
    };

    template<typename T, class Container = std::deque<T>>
    class BlockingQueue {
    public:
        // capacity == 0 means queue is unbounded
        explicit BlockingQueue() {
        }

        // throws QueueClosed exception after Close
        void put(T item) {
            std::unique_lock<std::mutex> lock(lock_);

            if (closed_) {
                throw QueueClosed();
            }

            items_.push_back(std::move(item));
            consumer_cv_.notify_one();
        }

        // returns false if queue is empty and closed
        bool get(T &item) {
            std::unique_lock<std::mutex> lock(lock_);

            while (!closed_ && isEmpty()) {
                consumer_cv_.wait(lock);
            }

            if (isEmpty()) {
                return false;
            }

            item = std::move(items_.front());
            items_.pop_front();

            return true;
        }

        bool get(std::vector<T>& out_items, const size_t max_count, bool require_at_least_one) {
            if (!max_count)
                return true;

            std::unique_lock<std::mutex> lock(lock_);

            if (require_at_least_one) {
                while (!closed_ && isEmpty()) {
                    consumer_cv_.wait(lock);
                }

                if (isEmpty()) {
                    return false;
                }
            }

            size_t count = std::min(max_count, items_.size());
            for (int i = 0; i < count; ++i) {
                out_items.push_back(std::move(items_.front()));
                items_.pop_front();
            }

            return true;
        }

        void close() {
            std::unique_lock<std::mutex> lock(lock_);

            closed_ = true;
            consumer_cv_.notify_all();
        }

        bool isEmpty() const {
            return items_.empty();
        }

    private:
        Container items_;
        bool closed_{false};
        std::mutex lock_;
        std::condition_variable consumer_cv_;
    };


    template<template<class> class Promise,
            template<class> class Future>
    class TimeMachine {
    public:
        explicit TimeMachine(size_t number_of_threads = 1) {
            for (int i = 0; i < number_of_threads; ++i) {
                runThread();
            }
        }

        TimeMachine(const TimeMachine<Promise, Future> &) = delete;

        TimeMachine(TimeMachine<Promise, Future> &&machine)
                : queue_(std::move(machine.queue_)) {}

        TimeMachine &operator=(const TimeMachine<Promise, Future> &) = delete;

        TimeMachine &operator=(TimeMachine<Promise, Future> &&machine) {
            queue_ = std::move(machine.queue_);
        }


        template<class ElemFrom, class ElemTo>
        Future<ElemTo> then(Future<ElemFrom> &&old_future, std::function<ElemTo(Future<ElemFrom> &&)> func) {
            Promise<ElemTo> *promise = new Promise<ElemTo>();
            Future<ElemFrom> *future = new Future<ElemFrom>(std::move(old_future));
            std::function<ElemTo(Future<ElemFrom> &&)>* func_ptr = new std::function<ElemTo(Future<ElemFrom> &&)>(std::move(func));
            Future<ElemTo> new_future = promise->get_future();

            queue_->put(std::make_pair(
                    [future](const std::chrono::milliseconds &timeout_duration) {
                        return future->wait_for(timeout_duration) == std::future_status::ready;
                    },
                    [future, promise, func_ptr]() {
                        try {
                            try {
                                ElemTo to = (*func_ptr)(std::move(*future));
                                promise->set_value(std::move(to));
                            } catch (...) {
                                promise->set_exception(std::current_exception());
                            }
                            delete promise;
                            delete future;
                            delete func_ptr;
                        } catch (...) {
                            delete promise;
                            delete future;
                            delete func_ptr;
                        }
                    }
            ));

            return new_future;
        }

        template<class ElemFrom>
        Future<void> then(Future<ElemFrom> &&old_future, std::function<void(Future<ElemFrom> &&)> func) {
            Promise<void> *promise = new Promise<void>();
            Future<ElemFrom> *future = new Future<ElemFrom>(std::move(old_future));
            std::function<void(Future<ElemFrom> &&)>* func_ptr = new std::function<void(Future<ElemFrom> &&)>(std::move(func));
            Future<void> new_future = promise->get_future();

            queue_->put(std::make_pair(
                    [future](const std::chrono::milliseconds &timeout_duration) {
                        return future->wait_for(timeout_duration) == std::future_status::ready;
                    },
                    [future, promise, func_ptr]() {
                        try {
                            try {
                                (*func_ptr)(std::move(*future));
                                promise->set_value();
                            } catch (...) {
                                promise->set_exception(std::current_exception());
                            }
                            delete promise;
                            delete future;
                            delete func_ptr;
                        } catch (...) {
                            delete promise;
                            delete future;
                            delete func_ptr;
                        }
                    }
            ));

            return new_future;
        }

        ~TimeMachine() {
            queue_->close();
            while (!queue_->isEmpty())
                std::this_thread::yield();
            while (state_->load());
        }


    private:
        using QueueData = std::pair<std::function<bool(const std::chrono::milliseconds &)>, std::function<void()>>;

        void runThread() {
            std::thread([this]() {
                std::vector<QueueData> picked;
                state_->fetch_add(1);

                while (queue_->get(picked, objects_per_thread_ - picked.size(), picked.empty())) {
                    processObjects(picked);
                }

                state_->fetch_sub(1);
            }).detach();
        }

        void processObjects(std::vector<QueueData> &picked) const {
            for (auto it = picked.begin(); it < picked.end(); ++it) {
                if (it->first(std::chrono::milliseconds(wait_for_one_object_ms_))) {
                    it->second();
                    picked.erase(it);
                }
            }
        }

    private:
        std::unique_ptr<BlockingQueue<QueueData>> queue_ = std::make_unique<BlockingQueue<QueueData>>();
        std::unique_ptr<std::atomic<long long>> state_ = std::make_unique<std::atomic<long long>>(0);
        const size_t objects_per_thread_ = 10;
        const size_t wait_for_one_object_ms_ = 200;
    };
}
