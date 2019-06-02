liboffkv
=================
liboffkv is a C++ library.

## Supported platforms

The library is currently tested on

- Ubuntu 18.04

  Full support.

- MacOS

  Full support.

- Windows 10

  Only Consul is supported.

## Dependencies

  - C++ compiler

    Currently tested compilers are

    - VS 2019
    - g++ 7.4.0
    - clang

    VS 2017 is known to fail.

  - [CMake](https://cmake.org)

    We suggest using cmake bundled with vcpkg.

  - [vcpkg](https://docs.microsoft.com/en-us/cpp/build/vcpkg)

## Developer workflow

- Copy all ports from `liboffkv/vcpkg/ports/` to `[vcpkg root]/ports` or create corresponding symbolic links.

  Note that the original vcpkg curl port is broken. Use fixed version from this repo.

- Install dependencies

  ```sh
  # from vcpkg root
  vcpkg install ppconsul etcdpp zkpp
  ```

  Installing all three packages is not required. See control flags at the next step.

- Build tests

    ```sh
    # from liboffkv directory
    mkdir cmake-build-debug && cd $_
    cmake -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_TOOLCHAIN_FILE='<replace with path to vcpkg.cmake>" \
          -DBUILD_TESTS=ON ..
    cmake --build .
    ```

    You can control the set of supported services with the following flags

    - `-DBUILD_ZK=[ON|OFF]`
    - `-DBUILD_ETCD=[ON|OFF]`
    - `-DBUILD_CONSUL=[ON|OFF]`

    Sometimes you may also need to specify `VCPKG_TARGET_TRIPLET`.

- Run tests

  ```sh
  # from liboffkv/cmake-build-debug directory
  make test
  ```