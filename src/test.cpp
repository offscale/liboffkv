#include <iostream>


#include "client.hpp"
#include "util/time_machine.hpp"

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
        std::cout << f.get();
    });

    a.set_value(10);
}

int main() {
//    test_client(connect("consul://127.0.0.1:8500"));
//    test_client(connect("zk://127.0.0.1:2181"));

    test_time_machine();
}
