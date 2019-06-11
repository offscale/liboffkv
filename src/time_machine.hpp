#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>


namespace liboffkv { namespace time_machine {

class QueueClosed : public std::runtime_error {
public:
    QueueClosed()
        : std::runtime_error("Queue closed for Puts")
    {}
};


template <typename T, class Container = std::deque<T>>
class BlockingQueue {
public:
    explicit BlockingQueue() = default;

    // throws QueueClosed exception after Close
    template <typename U>
    void put(U&& item)
    {
        std::unique_lock<std::mutex> lock(lock_);

        if (closed_) {
            throw QueueClosed();
        }

        items_.push_back(std::forward<U>(item));
        consumer_cv_.notify_one();
    }

    // returns false if queue is empty and closed
    bool get(T& item)
    {
        std::unique_lock<std::mutex> lock(lock_);

        while (!closed_ && isEmpty())
            consumer_cv_.wait(lock);

        if (isEmpty())
            return false;

        item = std::move(items_.front());
        items_.pop_front();

        return true;
    }

    bool get(std::vector<T>& out_items, size_t max_count, bool require_at_least_one)
    {
        if (!max_count)
            return true;

        std::unique_lock<std::mutex> lock(lock_);

        if (require_at_least_one) {
            while (!closed_ && isEmpty())
                consumer_cv_.wait(lock);

            if (isEmpty())
                return false;
        }

        const size_t count = std::min(max_count, items_.size());
        for (size_t i = 0; i < count; ++i) {
            out_items.push_back(std::move(items_.front()));
            items_.pop_front();
        }

        return true;
    }

    void close()
    {
        std::unique_lock<std::mutex> lock(lock_);

        closed_ = true;
        consumer_cv_.notify_all();
    }

    bool isEmpty() const
    {
        return items_.empty();
    }

private:
    Container items_;
    bool closed_{false};
    std::mutex lock_;
    std::condition_variable consumer_cv_;
};


template <template <class> class Promise = std::promise,
    template <class> class Future  = std::future>
class ThreadPool {
public:
    explicit ThreadPool(size_t number_of_threads = 1,
                        size_t objects_per_thread = 5, unsigned long long wait_for_object_ms = 3)
        : objects_per_thread_(objects_per_thread), wait_for_object_ms_(wait_for_object_ms)
    {
        for (size_t i = 0; i < number_of_threads; ++i)
            run_thread();
    }

    ThreadPool(const ThreadPool&) = delete;

    ThreadPool(ThreadPool&& machine)
        : queue_(std::move(machine.queue_)), state_(std::move(machine.state_)),
          objects_per_thread_(machine.objects_per_thread_),
          wait_for_object_ms_(machine.wait_for_object_ms_)
    {}

    ThreadPool& operator=(const ThreadPool&) = delete;

    ThreadPool& operator=(ThreadPool&& machine)
    {
        queue_ = std::move(machine.queue_);
        state_ = std::move(machine.state_);
        objects_per_thread_ = machine.objects_per_thread_;
        wait_for_object_ms_ = machine.wait_for_object_ms_;

        return *this;
    }


    template <typename T, class Function>
    Future<std::result_of_t<Function(Future<T>&&)>>
    then(Future<T>&& future, Function&& func, const bool run_extra_thread = false)
    {
        auto promise = std::make_shared<Promise<std::result_of_t<Function(Future<T>&&)>>>();
        auto future_ptr = std::make_shared<Future<T>>(std::move(future));
        auto new_future = promise->get_future();

        std::shared_ptr<std::atomic<bool>> allow_death;
        if (run_extra_thread) {
            allow_death = std::make_shared<std::atomic<bool>>(false);
            run_thread(allow_death);
        }

        queue_->put(std::make_pair(
            [future_ptr](std::chrono::milliseconds timeout_duration) mutable {
                return future_ptr->wait_for(timeout_duration) == std::future_status::ready;
            },
            [future_ptr, promise = std::move(promise), finished_flag = std::move(allow_death),
                func = std::forward<Function>(func)]() mutable {
                try {
                    if constexpr (std::is_same_v<void, std::result_of_t<Function(Future<T>&&) >>) {
                        func(std::move(*future_ptr));
                        promise->set_value();
                    } else {
                        auto to = func(std::move(*future_ptr));
                        promise->set_value(std::move(to));
                    }
                } catch (...) {
                    try {
                        promise->set_exception(std::current_exception());
                    } catch (...) {}
                }

                if (finished_flag)
                    finished_flag->store(true, std::memory_order::memory_order_relaxed);
            }
        ));

        return new_future;
    }


    template <class Function, class... Args>
    std::future<std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>>
    async(Function&& func, Args&& ... args)
    {
        using T = std::invoke_result_t<std::decay_t<Function>, std::decay_t<Args>...>;
        auto promise = std::make_shared<std::promise<T>>();
        auto future = promise->get_future();

        queue_->put(std::make_pair(
            [](std::chrono::milliseconds timeout_duration) mutable {
                return true;
            },
            [promise = std::move(promise), func = std::forward<Function>(func),
                args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
                try {
                    if constexpr (std::is_same_v<void, T>) {
                        std::apply(std::forward<Function>(func), std::move(args));
                        promise->set_value();
                    } else {
                        auto to = std::apply(std::forward<Function>(func), std::move(args));
                        promise->set_value(std::move(to));
                    }
                } catch (...) {
                    try {
                        promise->set_exception(std::current_exception());
                    } catch (...) {}
                }
            }
        ));

        return future;
    }


    // Function must be CopyConstructible
    template <class Function, class Time>
    void periodic(Function&& func, const Time& duration)
    {
        using Ms = std::chrono::milliseconds;
        using Clock = std::chrono::steady_clock;
        using TimePoint = Clock::time_point;

        std::shared_ptr<Ms> duration_ms = std::make_shared<Ms>(std::chrono::duration_cast<Ms>(duration));

        std::shared_ptr<QueueData> self = std::make_shared<QueueData>();
        std::shared_ptr<TimePoint> start_time = std::make_shared<TimePoint>(Clock::now());
        self->first =
            [duration = duration_ms, start_time](std::chrono::milliseconds timeout_duration) mutable {
                Ms duration_ms = *duration;
                TimePoint now = Clock::now();

                Ms passed = std::chrono::duration_cast<Ms>(now - *start_time);
                if (passed < duration_ms)
                    std::this_thread::sleep_for(std::min(duration_ms - passed, timeout_duration));

                passed = std::chrono::duration_cast<Ms>(now - *start_time);
                if (passed >= duration_ms)
                    return true;

                return false;
            };
        self->second =
            [self, start_time, duration = duration_ms, func = std::forward<Function>(func), queue = queue_]() mutable {
                try {
                    if constexpr (std::is_same_v<std::invoke_result_t<std::decay_t<Function>>, void>) {
                        func();
                    } else {
                        *duration = std::chrono::duration_cast<Ms>(func());
                        if (*duration == Ms::zero())
                            return;
                    }
                } catch (...) {}
                *start_time = Clock::now();
                try {
                    queue->put(*self);
                } catch (QueueClosed&) {}
            };

        queue_->put(*self);
    }

    ~ThreadPool()
    {
        if (queue_) {
            queue_->close();
            while (!queue_->isEmpty())
                std::this_thread::yield();
            while (state_->load())
                std::this_thread::yield();
        }
    }


private:
    using QueueData = std::pair<std::function<bool(std::chrono::milliseconds)>, std::function<void()>>;

    void run_thread(std::shared_ptr<std::atomic<bool>> allow_death = nullptr)
    {
        std::thread([state_ = this->state_, queue_ = this->queue_, allow_death = std::move(allow_death),
                        objects_per_thread_ = this->objects_per_thread_, wait_for_object_ms_ = this->wait_for_object_ms_] {
            std::vector<QueueData> picked;
            state_->fetch_add(1);

            while (queue_->get(picked, objects_per_thread_ - picked.size(), picked.empty())) {
                process_objects_(picked, wait_for_object_ms_);

                if (picked.empty() && allow_death && allow_death->load(std::memory_order::memory_order_relaxed))
                    break;
            }

            state_->fetch_sub(1);
        }).detach();
    }

    static void process_objects_(std::vector<QueueData>& picked, size_t wait_for_object_ms_)
    {
		std::vector<size_t> erase_it;
        for (auto it = picked.begin(); it < picked.end(); ++it) {
            if (it->first(std::chrono::milliseconds(wait_for_object_ms_))) {
                it->second();
				erase_it.push_back(it - picked.begin());
            }
        }

		std::reverse(erase_it.begin(), erase_it.end());
		for (auto index : erase_it) {
			picked.erase(picked.begin() + index);
		}
    }

private:
    std::shared_ptr<BlockingQueue<QueueData>> queue_ = std::make_shared<BlockingQueue<QueueData>>();
    std::shared_ptr<std::atomic<long long>> state_ = std::make_shared<std::atomic<long long >>(0);
    size_t objects_per_thread_;
    unsigned long long wait_for_object_ms_;
};

}} // namespace time_machine, liboffkv
