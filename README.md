liboffkv
========
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/offscale/liboffkv.svg?branch=master)](https://travis-ci.org/offscale/liboffkv)

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
  vcpkg install ppconsul etcdcpp zkpp
  ```

  Installing all three packages is not required. See control flags at the next step.

- Build tests

    ```sh
    # from liboffkv directory
    mkdir cmake-build-debug && cd $_
    cmake -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_TOOLCHAIN_FILE="<replace with path to vcpkg.cmake>" \
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

## Usage
```cpp
    #include "client.hpp"
    #include "error.hpp"
    #include "result.hpp"
    
    
    int main() {
        // specify protocol (zk | consul | etcd) and address to connect to the service
        // you can also specify a prefix all the keys will start with
        auto client = connect("consul://127.0.0.1:8500", "/prefix");
        
        // each method returns a future with
        std::future<CreateResult> result = client->create("/key", "value");
        
        // sometimes it is returned with an exception (for more details see "src/client_interface.hpp"
        try {
            std::cout << "Key \"/prefix/key\" created successfully! "
                      << "Its initial version is " << result.get().version << std::endl;
        } catch (EntryExists&) {
            std::cout << "Error: key \"/prefix/key\" already exists!" << std::endl;
        }
        
        
        // commit example (n.b. checks and other ops are separated from each other)
        client->commit(
            {
                {
                    op::Check("/key", 42u),
                    op::Check("/foo"),
                },
                {
                    op::erase("/key"),
                    op::set("/foo", "new_value"),
                }
            }
        ).get();
    }
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

