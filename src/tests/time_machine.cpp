//
// Created by alloky on 24.05.19.
//

#include <iostream>

#include "gtest/gtest.h"

#include "time_machine.hpp"


using namespace liboffkv;

TEST(time_machine_test, test_1)
{
    time_machine::ThreadPool<std::promise, std::future> timeMachine;

    int next_a_val = 10;
    int next_b_val = 5;

    std::promise<int> a;
    timeMachine.then(timeMachine.then(a.get_future(), [](auto&& f) {
        return f.get();
    }), [next_a_val](auto&& future_a) {
        ASSERT_EQ(future_a.get(), next_a_val);
    });

    std::promise<int> b;
    std::future<int> future_b = timeMachine.then(b.get_future(), [](auto&& f) -> int {
        throw std::runtime_error("My error!");
    });

    a.set_value(next_a_val);
    b.set_value(next_b_val);

    ASSERT_THROW(future_b.get(), std::runtime_error);

    auto intFuture = timeMachine.async(
            [](int a, int b, int c) -> int {
                return c;
            },
            1, 2, 3);
    timeMachine.then(
            std::move(intFuture),
            [](std::future<int>&& future_c) {
                ASSERT_EQ(future_c.get(), 3);
            }
    );

    short interval = 100;
    short time_eps = 12;
    auto prev = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());

    timeMachine.periodic([
            l = std::make_shared<int>(0),
            &prev,
            &interval,
            &time_eps] {
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
        auto last = prev;
        prev = ms;
        ASSERT_GE(ms.count() - last.count(), interval - time_eps);
        ASSERT_LE(ms.count() - last.count(), interval + time_eps);
    }, std::chrono::milliseconds(interval));

    std::this_thread::sleep_for(std::chrono::seconds(3));
}
