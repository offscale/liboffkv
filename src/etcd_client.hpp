#pragma once

#include "client_interface.hpp"



class ETCDClient : public Client {
public:
    ETCDClient(const std::string& address) {}


    std::string get(const std::string& key, const std::string& default_value) const {}
    void set(const std::string& key, const std::string& value) {}
};
