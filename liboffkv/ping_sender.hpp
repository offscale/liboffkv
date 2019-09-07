#pragma once

#include <condition_variable>
#include <mutex>
#include <chrono>
#include <thread>

namespace liboffkv::detail {

class PingControl
{
    std::condition_variable cv_;
    std::mutex mtx_;
    bool closed_;

public:
    PingControl()
        : cv_{}
        , mtx_{}
        , closed_{false}
    {}

    bool wait(std::chrono::seconds timeout)
    {
        std::unique_lock<std::mutex> lock(mtx_);
        return cv_.wait_for(lock, timeout, [this]() { return closed_; });
    }

    void close()
    {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            closed_ = true;
        }
        cv_.notify_one();
    }
};

class PingSender
{
    PingControl *ctl_; // this belongs to a thread.

    void destroy_() { if (ctl_) ctl_->close(); }

public:
    PingSender() : ctl_{nullptr} {}

    template<class Callback>
    PingSender(std::chrono::seconds first_timeout, Callback &&callback)
        : ctl_{new PingControl{}}
    {
        try {
            std::thread([ctl = ctl_, first_timeout, callback = std::forward<Callback>(callback)]() {
                auto timeout = first_timeout;
                while (!ctl->wait(timeout))
                    timeout = callback();
                delete ctl;
            }).detach();
        } catch (...) {
            delete ctl_;
            throw;
        }
    }

    PingSender(const PingSender &) = delete;

    PingSender(PingSender &&that)
        : ctl_{that.ctl_}
    {
        that.ctl_ = nullptr;
    }

    PingSender& operator =(const PingSender &) = delete;

    PingSender& operator =(PingSender &&that)
    {
        destroy_();
        ctl_ = that.ctl_;
        that.ctl_ = nullptr;
        return *this;
    }

    operator bool() const { return !!ctl_; }

    ~PingSender() { destroy_(); }
};

} // namespace liboffkv
