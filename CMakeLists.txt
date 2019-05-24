cmake_minimum_required(VERSION 3.0)
project(liboffkv VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 17)

option(ENABLE_ZK "Build with ZooKeeper support" ON)
option(ENABLE_ETCD "Build with etcd support" ON)
option(ENABLE_CONSUL "Build with Consul support" ON)
option(BUILD_TESTS "Build library tests" ON)

configure_file(lib/version.h.in generated/version.h @ONLY)
configure_file(lib/config.h.in generated/config.h @ONLY)

find_package (Threads REQUIRED)
link_libraries(${CMAKE_THREAD_LIBS_INIT})

add_library(liboffkv INTERFACE)
target_include_directories(liboffkv INTERFACE src)
target_include_directories(liboffkv INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/generated)

set(SERVICE_TEST_ADDRESSES)

if (${ENABLE_CONSUL})
    find_package(ppconsul REQUIRED)

    target_link_libraries(liboffkv INTERFACE ppconsul)

    list(APPEND SERVICE_TEST_ADDRESSES "consul://localhost:8500")
endif()

if (${ENABLE_ZK})
    find_package(zkpp REQUIRED)

    target_link_libraries(liboffkv INTERFACE zkpp)

    list(APPEND SERVICE_TEST_ADDRESSES "zk://localhost:2181")
endif()

if (${ENABLE_ETCD})
    find_package(etcdpp REQUIRED)
    find_package(gRPC CONFIG REQUIRED)

    target_link_libraries(liboffkv INTERFACE etcdpp)
    target_link_libraries(liboffkv INTERFACE gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)

    list(APPEND SERVICE_TEST_ADDRESSES "etcd://localhost:2379")
endif()


if (${BUILD_TESTS})
    # google tests
    enable_testing()
    find_package(GTest REQUIRED)

    function(create_test)
        set(ONE_VAL_ARGS NAME)
        set(MULTI_VAL_ARGS FILES COMPILE_DEFINITIONS)
        cmake_parse_arguments(ARG "" "${ONE_VAL_ARGS}" "${MULTI_VAL_ARGS}" ${ARGN})

        add_executable(test_${ARG_NAME} ${ARG_FILES})
        target_link_libraries(test_${ARG_NAME} liboffkv)
        target_link_libraries(test_${ARG_NAME} ${GTEST_BOTH_LIBRARIES})

        if (NOT "${ARG_COMPILE_DEFINITIONS}" STREQUAL "")
            target_compile_definitions(test_${ARG_NAME} PRIVATE ${ARG_COMPILE_DEFINITIONS})
        endif()

        add_test(${ARG_NAME} test_${ARG_NAME})
    endfunction()

    create_test(
            NAME time_machine
            FILES src/tests/time_machine.cpp
    )

    foreach(service_addr ${SERVICE_TEST_ADDRESSES})
        string(REGEX MATCH "^[a-zA-Z0-9]+" service_name "${service_addr}")
        create_test(
                NAME ${service_name}
                FILES src/tests/gtests.cpp
                COMPILE_DEFINITIONS "SERVICE_ADDRESS=\"${service_addr}\""
        )
    endforeach()
endif()

add_executable(test_lib src/test.cpp)
target_link_libraries(test_lib liboffkv)

#add_executable(playground src/playground.cpp)
#target_link_libraries(playground zkpp)
#target_link_libraries(playground ppconsul)
#target_link_libraries(playground etcdpp)
#target_link_libraries(playground gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)
