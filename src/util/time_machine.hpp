#pragma once

#include <condition_variable>
#include <mutex>
#include <utility>
#include <deque>


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

        void close() {
            std::unique_lock<std::mutex> lock(lock_);

            closed_ = true;
            consumer_cv_.notify_all();
        }

    private:
        bool isEmpty() const {
            return items_.empty();
        }

    private:
        Container items_;
        bool closed_{false};
        std::mutex lock_;
        std::condition_variable consumer_cv_;
    };


    template<template<class Elem> class Promise,
            template<class Elem> class Future>
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
            future->wait_for(std::chrono::milliseconds(10));
            Future<ElemTo> new_future = promise->get_future();

            queue_.put(std::make_pair(
                    [future](const std::chrono::milliseconds &timeout_duration) {
                        return future->wait_for(timeout_duration) == std::future_status::ready;
                    },
                    [future, promise, &func]() {
                        promise->set_value(func(std::move(*future)));
                        delete promise;
                        delete future;
                    }
            ));

            return new_future;
        }

        template<class ElemFrom>
        Future<void> then<ElemFrom, void>(Future<ElemFrom> &&old_future, std::function<void(Future<ElemFrom> &&)> func) {
            Promise<void> *promise = new Promise<void>();
            Future<ElemFrom> *future = new Future<ElemFrom>(std::move(old_future));
            future->wait_for(std::chrono::milliseconds(10));
            Future<void> new_future = promise->get_future();

            queue_.put(std::make_pair(
                    [future](const std::chrono::milliseconds &timeout_duration) {
                        return future->wait_for(timeout_duration) == std::future_status::ready;
                    },
                    [future, promise, &func]() {
                        promise->set_value();
                        delete promise;
                        delete future;
                    }
            ));

            return new_future;
        }

        ~TimeMachine() {
            queue_.close();
        }


    private:
        void runThread() {
            std::thread([this]() {
                std::pair<std::function<bool(const std::chrono::milliseconds &)>, std::function<void()>> item;
                while (queue_.get(item)) {
                    while (!item.first(std::chrono::milliseconds(200))) {}
                    item.second();
                }
            }).detach();
        }

    private:
        BlockingQueue<std::pair<std::function<bool(const std::chrono::milliseconds &)>, std::function<void()>>> queue_;
    };
}
