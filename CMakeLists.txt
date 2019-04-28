cmake_minimum_required(VERSION 3.0)
project(liboffkv VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

configure_file(lib/version.h.in version.h @ONLY)


find_package(ppconsul REQUIRED)
find_package(zkpp REQUIRED)

add_library(liboffkv INTERFACE)
target_link_directories(liboffkv INTERFACE src)
target_link_libraries(liboffkv INTERFACE ppconsul)
target_link_libraries(liboffkv INTERFACE zkpp)

add_executable(test_lib src/test.cpp)
target_link_libraries(test_lib liboffkv)
