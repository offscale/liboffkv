cmake_minimum_required(VERSION 3.5)


file(MAKE_DIRECTORY "generated")
file(GLOB_RECURSE protos LIST_DIRECTORIES false RELATIVE "${CMAKE_SOURCE_DIR}/proto/" "*.proto")

execute_process(COMMAND which grpc_cpp_plugin
				WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/proto/"
				RESULT_VARIABLE p0_res
				OUTPUT_VARIABLE grpccpp
                ERROR_VARIABLE p0_err)
string(STRIP "${grpccpp}" grpccpp)

execute_process(COMMAND protoc -I . --grpc_out=../generated/ "--plugin=protoc-gen-grpc=${grpccpp}" ./rpc.proto
				WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/proto/"
				RESULT_VARIABLE p1_res
				OUTPUT_VARIABLE p1_out
                ERROR_VARIABLE p1_err)
execute_process(COMMAND protoc -I . --cpp_out=../generated/ ${protos}
				WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/proto/"
				RESULT_VARIABLE p2_res
				OUTPUT_VARIABLE p2_out
                ERROR_VARIABLE p2_err)

message(STATUS "protosource: ${CMAKE_SOURCE_DIR}/proto/")
message(STATUS "p0:ret ${p0_res}")
message(STATUS "p0:err ${p0_err}")
message(STATUS "p1:ret ${p1_res}")
message(STATUS "p1:out ${p1_out}")
message(STATUS "p1:err ${p1_err}")
message(STATUS "p2:ret ${p2_res}")
message(STATUS "p2:out ${p2_out}")
message(STATUS "p2:err ${p2_err}")

find_package(gRPC CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)

file(GLOB_RECURSE cpps LIST_DIRECTORIES false "generated/*.cc")
add_library(etcdpp SHARED ${cpps})
target_link_libraries(etcdpp PRIVATE gRPC::gpr gRPC::grpc gRPC::grpc++ gRPC::grpc_cronet)
target_link_libraries(etcdpp PRIVATE protobuf::libprotoc protobuf::libprotobuf)
target_include_directories(etcdpp PRIVATE "generated/")
target_include_directories(etcdpp INTERFACE "${CMAKE_INSTALL_PREFIX}/include/etcdpp/")

file(GLOB_RECURSE hpps LIST_DIRECTORIES false "generated/*.h")

install(TARGETS etcdpp EXPORT etcdpp DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${hpps} DESTINATION include/etcdpp/)
install(EXPORT etcdpp DESTINATION share/etcdpp/ FILE etcdppConfig.cmake)
