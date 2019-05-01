#include <iostream>


#include "client.hpp"
#include "time_machine.hpp"


void test_time_machine();

template <typename TimeMachine>
void test_client(std::unique_ptr<Client<TimeMachine>>&& client) {
    auto version = client->set("key", "valueqq").get().version;
    std::cout << version << "\n";
//    tm->then(client->cas("key", "value1", 111), [](auto&& res) -> void {std::cout << "changed: " << !!res.get() << "\n";});
    try {
        std::cout << client->get("key").get().value;
    } catch (std::exception& e) {
        std::cout << e.what();
    }
//    tm->then(client->cas("key", "value1", version), [](auto&& res) -> void {std::cout << "changed: " << !!res.get() << "\n";});
    try {
        std::cout << client->get("key").get().value;
    } catch (std::exception& e) {
        std::cout << e.what();
    }
}


void test_time_machine() {
    time_machine::TimeMachine<std::promise, std::future> timeMachine;

    std::promise<int> a;
    timeMachine.then(timeMachine.then(a.get_future(), [](auto &&f) {
        return "Ready: " + std::to_string(f.get());
    }), [](auto&& f) {
        std::cout << f.get() << std::endl;
    });

    std::promise<int> b;
    std::future<int> future_b = timeMachine.then(b.get_future(), [](auto &&f) -> int {
        throw std::runtime_error("My error!");
    });

    a.set_value(10);
    b.set_value(5);

    try {
        future_b.get();
    } catch (const std::runtime_error& err) {
        std::cout << err.what() << std::endl;
    }
}

int main() {
//    test_client(connect("consul://127.0.0.1:8500"));
    test_client(connect("zk://127.0.0.1:2181"));

    test_time_machine();
}
