//
// Created by alloky on 07.06.19.
//

#include "test_client_fixture.hpp"

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>

#include <random>

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

void increaser(std::vector<std::string> keys, int n_inserts, boost::barrier& start_barier, int thread_id){

    std::uniform_int_distribution<int> dice_distribution(0, keys.size() - 1);
    std::mt19937 random_number_engine(thread_id); // pseudorandom number generator
    auto index_roller = [&dice_distribution, &random_number_engine]() {
        return dice_distribution(random_number_engine);
    };

    std::mt19937_64 time_eng{std::random_device{}()};  // or seed however you want
    std::uniform_int_distribution<> time_dist{10, 100};
    auto time_roller = [&time_dist, &time_eng]() {
        return time_dist(time_eng);
    };

    SingleClientConnection cn;
    start_barier.wait();

    for(int i = 0; i < n_inserts; ++i){
        std::this_thread::sleep_for(std::chrono::milliseconds{time_roller()});
        // choose rand key
        int index = index_roller();
        if(!cn.client->exists(keys[index]).get().exists){
            try {
//                std::cout << keys[index] << " \n";
                cn.client->create(keys[index], "10");
                RAW_LOG(INFO, "%d %d", index, 10);
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
//            std::cout << oldVal << " : " << newVal << " \n";
            auto casResult = cn.client->cas(keys[index], newVal, oldVersion).get();
            success = casResult.success;
        }
        LOG(INFO) << "w " << thread_id << " " << index << " " << newVal.c_str();
    }

}

void reader(std::vector<std::string> keys, int n_reads, boost::barrier& start_barier, int thread_id){
    std::uniform_int_distribution<int> dice_distribution(0, keys.size() - 1);
    std::mt19937 random_number_engine(thread_id); // pseudorandom number generator
    auto index_roller = [&dice_distribution, &random_number_engine]() {
        return dice_distribution(random_number_engine);
    };

    std::mt19937_64 time_eng{std::random_device{}()};  // or seed however you want
    std::uniform_int_distribution<> time_dist{10, 100};
    auto time_roller = [&time_dist, &time_eng]() {
        return time_dist(time_eng);
    };

    SingleClientConnection cn;
    start_barier.wait();
    for(int i = 0; i < n_reads; ++i){
        // choose rand key
        std::this_thread::sleep_for(std::chrono::milliseconds{time_roller()});
        int index = index_roller();
        try {
            auto getResult = cn.client->get(keys[index]).get();
            auto val = getResult.value;
            auto llval = str2ull(val);
            LOG(INFO) << "r " << thread_id << " " << index << " " << llval;
        } catch (liboffkv::NoEntry& e){}
    }

}


TEST(stress_test, increase_read_test)
{

    const int n = 10,
              n_incs = 100,
              n_readers = 4,
              n_writers = 4;

    google::SetLogDestination(google::INFO, "/tmp/liboffkv/increase_read_stress_test");
    google::InitGoogleLogging("liboffkv");

    SingleClientConnection cn;

    std::string base_key = "/stresstest";

    try {
        cn.client->erase(base_key).get();
    } catch (...) {}


    ASSERT_NO_THROW(cn.client->create(base_key, "value").get());


    std::string basic_path = base_key + "/1";

    std::vector<std::string> keys(n);

    for(int i = 0; i < keys.size(); ++i){
        keys[i] = basic_path + std::to_string(i+1);
//        std::cout << keys[i] << std::endl;
    }

    boost::barrier bar(n_readers + n_writers);

    std::vector<boost::thread> all(n_readers + n_writers);


    for(int i = 0; i < n_writers; ++i) {
        all[i] = boost::thread(boost::bind(&increaser, keys, n_incs, boost::ref(bar), i));
    }

    for(int i = n_writers; i < n_writers + n_readers; ++i) {
        all[i] = boost::thread(boost::bind(&reader, keys, n_incs, boost::ref(bar), i));
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
