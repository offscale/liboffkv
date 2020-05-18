cmake_minimum_required (VERSION 3.0)

project (liboffkv VERSION 0.0.1)

set (CMAKE_CXX_STANDARD 17)

option (ENABLE_ZK "Build with ZooKeeper support" ON)
option (ENABLE_ETCD "Build with etcd support" ON)
option (ENABLE_CONSUL "Build with Consul support" ON)
option (BUILD_TESTS "Build library tests" ON)
option (BUILD_ENV_TESTS "Build environment dependent library tests" OFF)
option (BUILD_CLIB "Build C library" OFF)
set(SANITIZE "" CACHE STRING "Build tests with sanitizer")

configure_file (liboffkv/version.hpp.in generated/liboffkv/version.hpp @ONLY)
configure_file (liboffkv/config.hpp.in generated/liboffkv/config.hpp @ONLY)

find_package (Threads REQUIRED)

if ("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic -Wpedantic -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers")
endif ()

add_library (liboffkv INTERFACE)
target_include_directories (liboffkv INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_link_libraries (liboffkv INTERFACE ${CMAKE_THREAD_LIBS_INIT})

if (BUILD_CLIB)
    add_library (liboffkv_c SHARED liboffkv/clib.cpp)
    target_link_libraries (liboffkv_c liboffkv)

    install(TARGETS liboffkv_c DESTINATION lib)
    install(FILES liboffkv/clib.h RENAME liboffkv.h DESTINATION include)
endif ()

set (SERVICE_TEST_ADDRESSES)

if (ENABLE_CONSUL)
    find_package (ppconsul REQUIRED)

    if (WIN32)
        find_package (ZLIB REQUIRED) # weird but works
    endif ()
    target_link_libraries (liboffkv INTERFACE ppconsul)

    list (APPEND SERVICE_TEST_ADDRESSES "consul://localhost:8500")
endif ()

if (ENABLE_ZK)
    find_package (zkpp REQUIRED)

    target_link_libraries (liboffkv INTERFACE zkpp)

    list (APPEND SERVICE_TEST_ADDRESSES "zk://localhost:2181")
endif ()

if (ENABLE_ETCD)
    find_package (etcdcpp REQUIRED)
    find_package (gRPC CONFIG REQUIRED)

    target_link_libraries (liboffkv INTERFACE etcdcpp)
    target_link_libraries (liboffkv INTERFACE gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)

    list (APPEND SERVICE_TEST_ADDRESSES "etcd://localhost:2379")
endif ()

if (BUILD_TESTS OR BUILD_ENV_TESTS)
    # google tests
    enable_testing ()
    add_subdirectory (tests)
endif ()
