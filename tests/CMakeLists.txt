find_package (GTest CONFIG REQUIRED)

function (create_test)
    set (ONE_VAL_ARGS NAME)
    set (MULTI_VAL_ARGS FILES COMPILE_DEFINITIONS LIBS)
    cmake_parse_arguments (ARG "" "${ONE_VAL_ARGS}" "${MULTI_VAL_ARGS}" ${ARGN})

    add_executable (test_${ARG_NAME} ${ARG_FILES} unit_tests_main.cpp)
    target_link_libraries (test_${ARG_NAME} liboffkv)
    target_link_libraries (test_${ARG_NAME} GTest::gtest)
    target_link_libraries (test_${ARG_NAME} ${ARG_LIBS})

    if (NOT "${ARG_COMPILE_DEFINITIONS}" STREQUAL "")
        target_compile_definitions (test_${ARG_NAME} PRIVATE ${ARG_COMPILE_DEFINITIONS})
    endif ()

    add_test (${ARG_NAME} test_${ARG_NAME})
endfunction ()


if (SANITIZE)
    message(STATUS "Use ${SANITIZE} sanitizer")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=${SANITIZE}")
    set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=${SANITIZE}")
endif()


if (BUILD_TESTS)
    # FIXME: build one executable and control sevice address via cmd arguments
    foreach (service_addr ${SERVICE_TEST_ADDRESSES})
        string (REGEX MATCH "^[a-zA-Z0-9]+" service_name "${service_addr}")
        create_test(
            NAME ${service_name}
            FILES unit_tests.cpp
            COMPILE_DEFINITIONS "SERVICE_ADDRESS=\"${service_addr}\"")
    endforeach()
endif()

if (BUILD_ENV_TESTS)
    create_test(
            NAME "conn"
            FILES connection_tests.cpp)
endif()
