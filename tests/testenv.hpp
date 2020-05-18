#pragma once

#include <stdlib.h>
#include <iostream>

class ConnectionControl {
public:
    static ConnectionControl& instance()
    {
        static ConnectionControl control{};
        return control;
    }

    void disconnect() {
        iptables("-P INPUT DROP");
        iptables("-P OUTPUT DROP");
    }

    void connect() {
        iptables("-P INPUT ACCEPT");
        iptables("-P OUTPUT ACCEPT");
    }

private:
    ConnectionControl() = default;

    void iptables(const std::string& params) {
        const std::string cmd = "iptables " + params;
        int res = system(cmd.c_str());
        if (res) {
            std::cerr << "'" << cmd << "' returned non-zero code. Abort" << std::endl;
            std::abort();
        }
    }
};
