#include <iostream>


#include "client.hpp"
#include "time_machine.hpp"

void test_time_machine();

void test_client(std::unique_ptr<Client> &&client) {
    client->set("key", "value");
    client->cas("key", "value1", 15);
    std::cout << client->get("key") << std::endl;
    client->cas("key", "value1", 5);
    std::cout << client->get("key") << std::endl;
}


void test_time_machine() {
    time_machine::TimeMachine<std::promise, std::future> timeMachine;

    std::promise<int> a;
    timeMachine.then<std::string>(timeMachine.then<int, std::string>(a.get_future(), [](auto &&f) {
        return "Ready: " + std::to_string(f.get());
    }), [](auto&& f) {
        std::cout << f.get() << std::endl;
    });

    std::promise<int> b;
    std::future<int> future_b = timeMachine.then<int, int>(b.get_future(), [](auto &&f) -> int {
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
//    test_client(connect("zk://127.0.0.1:2181"));

    test_time_machine();
}
