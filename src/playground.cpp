#include <future>
#include <thread>
#include <iostream>


int main() {

    std::promise<int> promise;
    std::future<int> f = promise.get_future();

    std::thread([p = std::move(promise)]() mutable {
        p.set_value(10);
    }).detach();

    std::cout << f.get() << std::endl;
    std::cout << f.get() << std::endl;
}