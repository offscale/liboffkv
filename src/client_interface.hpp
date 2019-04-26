#pragma once



class Client {
public:
    Client(const std::string&);
    Client() = default;
    
    virtual ~Client() = default;

    virtual void set(const std::string&, const std::string&) = 0;
    virtual std::string get(const std::string&, const std::string&) const = 0;
};
