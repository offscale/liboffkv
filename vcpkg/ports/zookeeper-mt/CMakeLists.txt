cmake_minimum_required(VERSION 3.5)

project(zookeeper VERSION 3.5.4)
set(email user@zookeeper.apache.org)
set(description "zookeeper C client")

# general options
if(UNIX)
  add_compile_options(-Wall -fPIC)
elseif(WIN32)
  add_compile_options(/W3)
endif()
add_definitions(-DUSE_STATIC_LIB)

# TODO: Enable /WX and /W4 on Windows. Currently there are ~1000 warnings.
# TODO: Add Solaris support.
# TODO: Add a shared library option.
# TODO: Specify symbols to export.
# TODO: Generate doxygen documentation.

# Sync API option
option(WANT_SYNCAPI "Enables Sync API support" ON)
if(WANT_SYNCAPI)
  add_definitions(-DTHREADED)
endif()

# CppUnit option
if(WIN32 OR APPLE)
  # The tests do not yet compile on Windows or macOS,
  # so we set this to off by default.
  #
  # Note that CMake does not have expressions except in conditionals,
  # so we're left with this if/else/endif pattern.
  set(DEFAULT_WANT_CPPUNIT OFF)
else()
  set(DEFAULT_WANT_CPPUNIT ON)
endif()
option(WANT_CPPUNIT "Enables CppUnit and tests" ${DEFAULT_WANT_CPPUNIT})

# SOCK_CLOEXEC
option(WANT_SOCK_CLOEXEC "Enables SOCK_CLOEXEC on sockets" OFF)
include(CheckSymbolExists)
check_symbol_exists(SOCK_CLOEXEC sys/socket.h HAVE_SOCK_CLOEXEC)
if(WANT_SOCK_CLOEXEC AND HAVE_SOCK_CLOEXEC)
  set(SOCK_CLOEXEC_ENABLED 1)
endif()


# The function `to_have(in out)` converts a header name like `arpa/inet.h`
# into an Autotools style preprocessor definition `HAVE_ARPA_INET_H`.
# This is then set or unset in `configure_file()` step.
#
# Note that CMake functions do not have return values; instead an "out"
# variable must be passed, and explicitly set with parent scope.
function(to_have in out)
  string(TOUPPER ${in} str)
  string(REGEX REPLACE "/|\\." "_" str ${str})
  set(${out} "HAVE_${str}" PARENT_SCOPE)
endfunction()

# include file checks
foreach(f generated/zookeeper.jute.h generated/zookeeper.jute.c)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${f}")
    to_have(${f} name)
    set(${name} 1)
  else()
    message(FATAL_ERROR
      "jute files are missing!\n"
      "Please run 'ant compile_jute' while in the ZooKeeper top level directory.")
  endif()
endforeach()

# header checks
include(CheckIncludeFile)
set(check_headers
  arpa/inet.h
  dlfcn.h
  fcntl.h
  inttypes.h
  memory.h
  netdb.h
  netinet/in.h
  stdint.h
  stdlib.h
  string.h
  strings.h
  sys/socket.h
  sys/stat.h
  sys/time.h
  sys/types.h
  unistd.h
  sys/utsname.h)

foreach(f ${check_headers})
  to_have(${f} name)
  check_include_file(${f} ${name})
endforeach()

# function checks
include(CheckFunctionExists)
set(check_functions
  getcwd
  gethostbyname
  gethostname
  getlogin
  getpwuid_r
  gettimeofday
  getuid
  memmove
  memset
  poll
  socket
  strchr
  strdup
  strerror
  strtol)

foreach(fn ${check_functions})
  to_have(${fn} name)
  check_function_exists(${fn} ${name})
endforeach()

# library checks
set(check_libraries rt m pthread)
foreach(lib ${check_libraries})
  to_have("lib${lib}" name)
  find_library(${name} ${lib})
endforeach()

# IPv6 check
include(CheckStructHasMember)
check_struct_has_member("struct sockaddr_in6" sin6_addr "netinet/in.h" ZOO_IPV6_ENABLED)

# configure
configure_file(cmake_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/config.h)

# hashtable library
set(hashtable_sources src/hashtable/hashtable_itr.c src/hashtable/hashtable.c)
add_library(hashtable STATIC ${hashtable_sources})
target_include_directories(hashtable 
  PUBLIC
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE include)
target_link_libraries(hashtable PUBLIC $<$<OR:$<PLATFORM_ID:Linux>,$<PLATFORM_ID:FreeBSD>>:m>)

# zookeeper library
set(zookeeper_sources
  src/zookeeper.c
  src/recordio.c
  generated/zookeeper.jute.c
  src/zk_log.c
  src/zk_hashtable.c
  src/addrvec.c)

if(WANT_SYNCAPI)
  list(APPEND zookeeper_sources src/mt_adaptor.c)
else()
  list(APPEND zookeeper_sources src/st_adaptor.c)
endif()

if(WIN32)
  list(APPEND zookeeper_sources src/winport.c)
endif()

add_library(zookeeper_mt STATIC ${zookeeper_sources})
target_include_directories(zookeeper_mt
  INTERFACE
    "${CMAKE_INSTALL_PREFIX}/include"
  PUBLIC
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/generated>
  PRIVATE include ${CMAKE_CURRENT_BINARY_DIR}/include generated)
target_link_libraries(zookeeper_mt PUBLIC
  hashtable
  $<$<PLATFORM_ID:Linux>:rt> # clock_gettime
  $<$<PLATFORM_ID:Windows>:ws2_32>) # Winsock 2.0

if(WANT_SYNCAPI AND NOT WIN32)
  find_package(Threads REQUIRED)
  target_link_libraries(zookeeper_mt PUBLIC Threads::Threads)
endif()

# cli executable
add_executable(cli src/cli.c)
target_link_libraries(cli zookeeper_mt)

# load_gen executable
if(WANT_SYNCAPI AND NOT WIN32)
  add_executable(load_gen src/load_gen.c)
  target_link_libraries(load_gen zookeeper_mt)
endif()

# tests
set(test_sources
  tests/TestDriver.cc
  tests/LibCMocks.cc
  tests/LibCSymTable.cc
  tests/MocksBase.cc
  tests/ZKMocks.cc
  tests/Util.cc
  tests/ThreadingUtil.cc
  tests/TestZookeeperInit.cc
  tests/TestZookeeperClose.cc
  tests/TestReconfig.cc
  tests/TestReconfigServer.cc
  tests/TestClientRetry.cc
  tests/TestOperations.cc
  tests/TestMulti.cc
  tests/TestWatchers.cc
  tests/TestClient.cc
  tests/ZooKeeperQuorumServer.cc
  tests/TestReadOnlyClient.cc)

if(WANT_SYNCAPI)
  list(APPEND test_sources tests/PthreadMocks.cc)
endif()

include(GNUInstallDirs)

install(TARGETS zookeeper_mt hashtable EXPORT zookeeper DESTINATION lib/)

if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
  install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/zookeeper")
  install(DIRECTORY "generated/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/zookeeper")
  install(DIRECTORY "include/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/zookeeper")

  install(EXPORT zookeeper DESTINATION share/zookeeper_mt/ FILE zookeeper_mtConfig.cmake)
endif()
