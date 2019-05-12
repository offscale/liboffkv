#include <iostream>


#include "client.hpp"
#include "key.hpp"
#include "operation.hpp"
#include "time_machine.hpp"



auto tm = std::make_shared<time_machine::ThreadPool<>>();

void test_time_machine();

void test_client(std::unique_ptr<Client>&& client)
{
    try {
        client->erase("/key").get();
    } catch (...) {}

    client->create("/key", "value").get();
    auto version = client->set("/key", "valueqq").get().version;

    tm->then(client->cas("/key", "value1", 111), [](auto&& cas_result) {
        auto result = cas_result.get();
        if (!!result) {
            std::cout << "cas finished successfully! new key's version: " << result.version << std::endl;
        } else {
            std::cout << "cas failed! key's version was not " << 111 << " but " << result.version << std::endl;
        }
    }).wait();

    std::cerr << client->get("/key").get().value << std::endl;

    tm->then(client->cas("/key", "value1", version), [version](auto&& cas_result) {
        auto result = cas_result.get();
        if (!!result) {
            std::cout << "cas finished successfully! new key's version: " << result.version << std::endl;
        } else {
            std::cout << "cas failed! key's version was not " << 111 << " but " << result.version << std::endl;
        }
    }).wait();

    std::cerr << client->get("/key").get().value << std::endl;

    client->erase("/key").get()/* doesn't work for consul !!*/;

//    client->commit(
//    {
//        {
//            op::Check("a", 0),
//            op::Check("tr_key", 0)
//        },
//        op::Create("tr_key", "value"),
//        op::Set("tr_key", "new_value"),
//        op::Erase("tr_key", 1)
//    });
}


template <class ClientConstructor>
void test_get_watches(const ClientConstructor& client_constructor)
{
    std::unique_ptr<Client> client = client_constructor();
    try {
        client->erase("/key").get();
    } catch (...) {}
    client->create("/key", "test1").get();
    
    // use mutex as simple barrier
    std::mutex my_lock;
    my_lock.lock();
    
    std::thread([&client_constructor, &my_lock]() mutable {
        std::unique_ptr<Client> client = client_constructor();
        std::lock_guard<std::mutex> lock_guard(my_lock);
        std::cout << "Watch, call set" << std::endl;
        client->set("/key", "test2").get();
        std::cout << "Watch, finish set" << std::endl;
    }).detach();

    std::future<GetResult> future = client->get("/key", true);
    my_lock.unlock();


    const GetResult get = future.get();
    std::cout << "Watch, got: " << get.value << std::endl;
    get.watch->get();
    std::cout << "Watch triggered" << std::endl;

    const GetResult get2 = client->get("/key").get();
    std::cout << "Watch, new value: " << get2.value << std::endl;
    std::cout << "Watch, ==nullptr?: " << (get2.watch.get() == nullptr) << std::endl;
}


void test_time_machine()
{
    time_machine::ThreadPool<std::promise, std::future> timeMachine;

    std::promise<int> a;
    timeMachine.then(timeMachine.then(a.get_future(), [](auto&& f) {
        return "Ready: " + std::to_string(f.get());
    }), [](auto&& f) {
        std::cout << f.get() << std::endl;
    });

    std::promise<int> b;
    std::future<int> future_b = timeMachine.then(b.get_future(), [](auto&& f) -> int {
        throw std::runtime_error("My error!");
    });

    a.set_value(10);
    b.set_value(5);

    try {
        future_b.get();
    } catch (const std::runtime_error& err) {
        std::cout << err.what() << std::endl;
    }

    auto intFuture = timeMachine.async(
        [](int a, int b, int c) {
            std::cout << "Async testing: " << a << ' ' << b << std::endl;
            return c;
        },
        1, 2, 3);
    timeMachine.then(
        std::move(intFuture),
        [](std::future<int>&& future) {
            std::cout << future.get() << std::endl;
        }
    );
}


void test_path_parse()
{
    std::string path = "/foo/bar/bax/kek";
    auto parsed = get_entry_sequence(path);
    for (const auto& key : parsed)
        std::cout << "<" << key << ">" << " ";
}


int main()
{
//    test_path_parse();
    test_get_watches([tm = tm]{return connect("zk://127.0.0.1:2181", "", tm);});
    test_get_watches([tm = tm]{return connect("zk://127.0.0.1:2181", "/test/the/prefix", tm);});
//    test_client(connect("etcd://127.0.0.1:2379", "", tm));

    test_time_machine();
}
