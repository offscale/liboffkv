cmake_minimum_required(VERSION 3.0)
project(liboffkv VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

option(ENABLE_ZK "Build with ZooKeeper support" ON)
option(ENABLE_ETCD "Build with etcd support" ON)
option(ENABLE_CONSUL "Build with Consul support" ON)

message(STATUS ${ENABLE_ETCD})

configure_file(lib/version.h.in generated/version.h @ONLY)
configure_file(lib/config.h.in generated/config.h @ONLY)

find_package (Threads REQUIRED)
link_libraries(${CMAKE_THREAD_LIBS_INIT})

add_library(liboffkv INTERFACE)
target_include_directories(liboffkv INTERFACE src)
target_include_directories(liboffkv INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/generated)

if (${ENABLE_CONSUL})
    find_package(ppconsul REQUIRED)
    target_link_libraries(liboffkv INTERFACE ppconsul)
endif()

if (${ENABLE_ZK})
    find_package(zkpp REQUIRED)
    target_link_libraries(liboffkv INTERFACE zkpp)
endif()

if (${ENABLE_ETCD})
    find_package(etcdpp REQUIRED)
    find_package(gRPC CONFIG REQUIRED)

    target_link_libraries(liboffkv INTERFACE etcdpp)
    target_link_libraries(liboffkv INTERFACE gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)
endif()


add_executable(test_lib src/test.cpp)
target_link_libraries(test_lib liboffkv)

add_executable(playground src/playground.cpp)
target_link_libraries(playground zkpp)
target_link_libraries(playground etcdpp)
target_link_libraries(playground gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)
