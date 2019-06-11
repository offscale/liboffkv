//
// Created by alloky on 07.06.19.
//

#include "test_client_fixture.hpp"

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>

#include "single_client_connection.hpp"

std::string strinc(std::string& old){
    size_t len = 0;
    unsigned long long val = std::stoull(old, &len);
    assert(len == old.size());
    return std::to_string(val + 1);
}

unsigned long long str2ull(std::string& s){
    size_t len = 0;
    unsigned long long llval = std::stoull(s, &len);
    assert(len == s.size());
    return llval;
}

void increaser(std::vector<std::string> keys, int n_inserts, boost::barrier& start_barier){
    SingleClientConnection cn;
    std::cout << "inc here" << std::endl;
    start_barier.wait();

    for(int i = 0; i < n_inserts; ++i){
        // choose rand key
        int index = rand() % keys.size();
        if(cn.client->exists(keys[index]).get().exists){
            try {
                cn.client->create(keys[index], "1");
//                RAW_LOG(INFO, "%d %d", index, 1);
                continue;
            } catch (liboffkv::NoEntry& e) { };
        }
        // cas before success
        bool success = false;
        std::string newVal;
        while(!success){
            auto getResult = cn.client->get(keys[index]).get();
            auto oldVal = getResult.value;
            auto oldVersion = getResult.version;
            // inc string
            newVal = strinc(oldVal);
            auto casResult = cn.client->cas(keys[index], newVal, oldVersion).get();
            success = casResult.success;
        }
//        RAW_LOG(INFO, "%d %s", index, newVal.c_str());
    }

    cn.client = std::move(cn.client);
}

void reader(std::vector<std::string> keys, int n_reads, boost::barrier& start_barier){
    SingleClientConnection cn;
    std::cout << "inc here" << std::endl;
    start_barier.wait();
    for(int i = 0; i < n_reads; ++i){
        // choose rand key
        int index = rand() % keys.size();
        auto getResult = cn.client->get(keys[index]).get();
        auto val = getResult.value;
        auto llval = str2ull(val);
//        RAW_LOG(INFO, "%d %llu", index, llval);
    }

}


TEST(stress_test, increase_read_test)
{

    const int n = 10,
              n_incs = 1e3,
              n_readers = 4,
              n_writers = 4;

//    google::InitGoogleLogging("increase_read_test_log.txt");

    SingleClientConnection cn;

    std::string base_key = "/stresstest";

    try {
        cn.client->erase(base_key).get();
    } catch (...) {}


    ASSERT_NO_THROW(cn.client->create(base_key, "value").get());


    std::string basic_path = base_key + "/1";

    std::vector<std::string> keys(n);

    for(int i = 1; i < keys.size(); ++i){
        keys[i] = basic_path + std::to_string(i);
        std::cout << keys[i] << std::endl;
    }

    boost::barrier bar(n_readers + n_writers);

    std::vector<boost::thread> all(n_readers + n_writers);


    for(int i = 0; i < n_writers; ++i) {
        all[i] = boost::thread(boost::bind(&increaser, keys, n_incs, boost::ref(bar)));
        all[i].detach();
    }

    for(int i = n_writers; i < n_writers + n_readers; ++i) {
        all[i] = boost::thread(boost::bind(&reader, keys, n_incs, boost::ref(bar)));
        all[i].detach();
    }

    for(auto & thr : all){
        thr.join();
    }

    for (const auto& key : keys) {
        try {
            cn.client->erase(key).get();
        } catch (liboffkv::NoEntry& exc) {}
    }


    cn.client->erase(base_key).get();
}
