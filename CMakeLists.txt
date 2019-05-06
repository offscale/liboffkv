cmake_minimum_required(VERSION 3.0)
project(liboffkv VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

configure_file(lib/version.h.in version.h @ONLY)


find_package(ppconsul REQUIRED)
find_package(zkpp REQUIRED)
find_package(etcdpp REQUIRED)
find_package(gRPC CONFIG REQUIRED)
#find_package(protobuf REQUIRED)

find_package (Threads REQUIRED)
link_libraries(${CMAKE_THREAD_LIBS_INIT})

add_library(liboffkv INTERFACE)
target_include_directories(liboffkv INTERFACE src)
target_link_libraries(liboffkv INTERFACE ppconsul)
target_link_libraries(liboffkv INTERFACE zkpp)
target_link_libraries(liboffkv INTERFACE etcdpp)
target_link_libraries(liboffkv INTERFACE gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)

add_executable(test_lib src/test.cpp)
target_link_libraries(test_lib liboffkv)

add_executable(playground src/playground.cpp)
target_link_libraries(playground etcdpp)
target_link_libraries(playground gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)
