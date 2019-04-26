#include <iostream>


#include "client.hpp"



int main()
{

{
    auto client = connect("consul://127.0.0.1:8500");
    client->set("key", "value");
    std::cout << client->get("key", "default");
}

{
    auto client = connect("zk://127.0.0.1:2181");
    client->set("key", "value");
    std::cout << client->get("key", "default");
}

}
