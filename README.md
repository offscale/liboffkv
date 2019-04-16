liboffkv
=================
liboffkv is a C++ library.

## Dependencies

  - C++ compiler
  - [CMake](https://cmake.org)
  - [vcpkg](https://docs.microsoft.com/en-us/cpp/build/vcpkg)

## Developer workflow

    mkdir cmake-build-debug && cd $_
    cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE='<replace with path to vcpkg.cmake>" ..
    cmake --build ..
