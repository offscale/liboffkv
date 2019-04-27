#include <iostream>


#include "client.hpp"



void test_client(std::unique_ptr<Client>&& client)
{
    client->set("key", "value");
    client->cas("key", "value1", 15);
    std::cout << client->get("key") << std::endl;
    client->cas("key", "value1", 5);
    std::cout << client->get("key") << std::endl;
}


int main()
{
    test_client(connect("consul://127.0.0.1:8500"));
    test_client(connect("zk://127.0.0.1:2181"));

}
