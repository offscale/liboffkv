cmake_minimum_required(VERSION 3.0)
project(liboffkv VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

option(ENABLE_ZK "Build with ZooKeeper support" ON)
option(ENABLE_ETCD "Build with etcd support" ON)
option(ENABLE_CONSUL "Build with Consul support" ON)
option(BUILD_TESTS "Build library tests" OFF)
option(BUILD_STRESS_TESTS "Build library tests" OFF)

configure_file(lib/version.h.in generated/version.h @ONLY)
configure_file(lib/config.h.in generated/config.h @ONLY)

find_package (Threads REQUIRED)
link_libraries(${CMAKE_THREAD_LIBS_INIT})

add_library(liboffkv INTERFACE)
target_include_directories(liboffkv INTERFACE src)
target_include_directories(liboffkv INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/generated)

set(SERVICE_TEST_ADDRESSES)

if (ENABLE_CONSUL)
    find_package(ppconsul REQUIRED)

    if (WIN32)
        find_package(ZLIB REQUIRED) # weird but works
    endif()
    target_link_libraries(liboffkv INTERFACE ppconsul)

    list(APPEND SERVICE_TEST_ADDRESSES "consul://localhost:8500")
endif()

if (ENABLE_ZK)
    find_package(zkpp REQUIRED)

    target_link_libraries(liboffkv INTERFACE zkpp)

    list(APPEND SERVICE_TEST_ADDRESSES "zk://localhost:2181")
endif()

if (ENABLE_ETCD)
    find_package(etcdcpp REQUIRED)
    find_package(gRPC CONFIG REQUIRED)

    target_link_libraries(liboffkv INTERFACE etcdcpp)
    target_link_libraries(liboffkv INTERFACE gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)

    list(APPEND SERVICE_TEST_ADDRESSES "etcd://localhost:2379")
endif()


if (BUILD_TESTS)
    # google tests
    enable_testing()
    add_subdirectory(tests)
endif()
