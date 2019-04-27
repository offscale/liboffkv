#pragma once


#include "operation.hpp"
#include "result.hpp"
#include "util.hpp"



class Client {
public:
    Client(const std::string&);
    Client() = default;

    virtual
    ~Client() = default;


    virtual
    void create(const std::string& key, const std::string& value, const std::string& lease) = 0;

    virtual
    bool exists(const std::string& key/*, Watch watch = nullptr*/) const = 0;

    virtual
    void set(const std::string& key, const std::string& value) = 0;

    virtual
    void cas(const std::string& key, const std::string& value, int64_t version) = 0;

    virtual
    std::string get(const std::string& key/*, Watch watch = nullptr*/) const = 0;

    virtual
    void erase(const std::string& key, int64_t version) = 0;

//    virtual
//    void transaction(const std::string& key, const std::vector<Operation>& ops) = 0;
};
