liboffkv
========
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Travis CI](http://badges.herokuapp.com/travis/offscale/liboffkv?branch=master&label=OSX&env=BADGE=osx&style=flat-square)](https://travis-ci.org/offscale/liboffkv)
[![Travis CI](http://badges.herokuapp.com/travis/offscale/liboffkv?branch=master&label=Linux&env=BADGE=linux&style=flat-square)](https://travis-ci.org/offscale/liboffkv)

#### The library is designed to provide a uniform interface for three distributed KV storages: etcd, ZooKeeper and Consul.

The services have similar but different data models, so we outlined the common features.

In our implementation, keys form a ZK-like hierarchy. Each key has a version that is int64 number greater than 0. Current version is returned with other data by the most of operations. All the operations supported are listed below. 

<table align="center">
  <thead>
    <tr>
      <th>Method</th>
      <th>Parameters</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>create</td>
      <td>
        <b>key:</b> string<br>
        <b>value:</b> char[]<br>
        <b>leased:</b> bool (=false) -- <i>makes the key to be deleted on client disconnect</i>
      </td>
      <td>Creates the key.<br>
          Throws an exception if the key already exists or<br>
          preceding entry does not exist.<br>
          <b>Returns:</b> version of the newly created key.
      </td>
    </tr>
    <tr>
        <td>set</td>
        <td><b>key:</b> string<br>
            <b>value:</b> char[]
        </td>
        <td>Assigns the value.<br>
            <u>Creates</u> the key if it doesn’t exist.<br>
            Throws an exception if preceding entry does not exist.<br>
            <b>Returns:</b> new version of the key.
        </td>
    </tr>
    <tr>
        <td>cas</td>
        <td><b>key:</b> string<br>
            <b>value:</b> char[]<br>
            <b>version:</b> int64 (=0) -- <i>expected version of the key</i>
        </td>
        <td>
            Compare and set operation.<br>
            If the key does not exist and the version passed equals 0, creates it.<br>
            Throws an exception if preceding entry does not exist.<br>
            If the key exists and its version equals to specified one updates the value.
            Otherwise does nothing and returns 0.<br>
            <b>Returns:</b> new version of the key or 0.
        </td>
    </tr>
    <tr>
        <td>get</td>
        <td><b>key:</b> string<br>
            <b>watch:</b> bool (=false) -- <i> start watching for change in value</i>
        </td>
        <td>
            Returns the value currently assigned to the key.<br>
            Throws an exception if the key does not exist.<br>
            If <b>watch</b> is true, creates WatchHandler waiting for a change in value. (see usage example below).<br>
            <b>Returns:</b> current value and WatchHandler.
        </td>
    </tr>
    <tr>
        <td>exists</td>
        <td><b>key:</b> string<br>
            <b>watch:</b> bool (=false) -- <i>start watching for removal or creation of the key</i>
        </td>
        <td>
            Checks if the key exists.<br>
            If <b>watch</b> is true, creates WatchHandler waiting for a change in state of existance (see usage example below).<br>
            <b>Returns:</b> version of the key or 0 if it doesn't exist and WatchHandler.
        </td>
    </tr>
    <tr>
        <td>get_children</td>
        <td><b>key:</b> string<br>
            <b>watch:</b> bool (=false)
        </td>
        <td>
        Returns a list of the key's <u>direct</u> children.<br>
        Throws an exception if the key does not exist.<br>
        If <b>watch</b> is true, creates WatchHandler waiting for any changes among the children.<br>
        <b>Returns:</b> list of direct children and WatchHandler.
        </td>
    </tr>
    <tr>
        <td>erase</td>
        <td><b>key:</b> string<br>
            <b>version:</b> int64 (=0)
        <td>
        Erases the key and <u>all its descendants</u> if the <b>version</b> given equals to the current key's version.<br>
        Does it unconditionally if <b>version</b> is 0.<br>
        Throws an exception if the key does not exist.<br>
        <b>Returns:</b> (void)
    </td>
    </tr>
    <tr>
        <td>commit</td>
        <td><b>transaction:</b> Transaction</td>
        <td>Commits transaction (see transactions API below).<br>
            If it was failed, throws TxnFailed with an index of the failed operation.<br>
            <b>Returns:</b> list of new versions of keys affected by the transaction</td>
    </tr>
  </tbody>
</table>

### Transactions
Transaction is a chain of operations of 4 types: create, set, erase, check, performing atomically. Their descriptions can be found below.
N.b. at the moment <b>set</b> has different behavior in comparison to ordinary <b>set</b>: when used in transaction, it does not create the key if it does not exist. Besides you cannot assign watches. Leases are still available. 
Transaction body is separated into two blocks: firstly you should write all required checks and then the sequence of other operations  (see an example below). As a result a list of new versions for all the keys involved in set operations is returned.
<table align="center">
  <thead>
    <tr>
      <th>Method</th>
      <th>Parameters</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>create</td>
      <td>
        <b>key:</b> string<br>
        <b>value:</b> char[]<br>
        <b>leased:</b> bool (=false)
      </td>
      <td>Creates the key.<br>
          Rolls back if the key already exists or preceding entry does not exist.<br>
      </td>
    </tr>
    <tr>
        <td>set</td>
        <td><b>key:</b> string<br>
            <b>value:</b> char[]
        </td>
        <td>Set the value.<br>
            Rolls back if the key does not exist.<br>
            <u>n.b behaviors of transaction set and ordinary set differ</u>
        </td>
    </tr>
    <tr>
        <td>erase</td>
        <td><b>key:</b> string<br>
            <b>version:</b> int64 (=0)<br>
        <td>
        Erases the key and <u>all its descendants</u><br>
        if the <b>version</b> passed equals to the current key's version.<br>
        Does it unconditionally if the <b>version</b> is 0.<br>
        Rolls back if the key does not exist.<br>
    </tr>
    <tr>
        <td>check</td>
        <td><b>key:</b> string<br>
            <b>version:</b> int64
        </td>
        <td>Checks if the given key has the specified version.<br>
        Only checks if it exists if the <b>version</b> is 0
    </tr>
  </tbody>
</table>

## Usage
```cpp
#include <iostream>
#include <thread>

#include <liboffkv/liboffkv.hpp>

using namespace liboffkv;


int main()
{
    // firstly specify protocol (zk | consul | etcd) and address
    // you can also specify a prefix all the keys will start with
    auto client = open("consul://127.0.0.1:8500", "/prefix");


    // on failure methods throw exceptions (for more details see "liboffkv/client.hpp")
    try {
        int64_t initial_version = client->create("/key", "value");
        std::cout << "Key \"/prefix/key\" was created successfully! "
                  << "Its initial version is " << initial_version
                  << std::endl;
    } catch (EntryExists&) {
        // other exception types can be found in liboffkv/errors.hpp
        std::cout << "Error: key \"/prefix/key\" already exists!"
                  << std::endl;
    }


    // WATCH EXAMPLE
    auto result = client.exists("/key", true);

    // this thread erase the key after 10 seconds
    std::thread([&client]() mutable {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        client->erase("/key");
    }).detach();

    // now the key exists
    assert(result);

    // wait for changes
    result.watch->wait();

    // if the waiting was completed, the existance state must be different
    assert(!client.exists("/key"));


    // TRANSACTION EXAMPLE
    // n.b. checks and other ops are separated from each other
    try {
        auto txn_result = client->commit(
            {
                // firstly list your checks
                {
                    TxnCheck("/key", 42u),
                    TxnCheck("/foo"),
                },
                // then a chain of ops that are to be performed
                // in case all checks are satisfied
                {
                    TxnErase("/key"),
                    TxnSet("/foo", "new_value"),
                }
            }
        );

        // only one set/create operation
        assert(txn_result.size() == 1 &&
               txn_result[0].kind == TxnOpResult::Kind::SET);

        std::cout << "After the transaction the new version of \"/foo\" is "
                  << txn_result[0].version << std::endl;
    } catch (TxnFailed& e) {
        // TxnFailed exception contains failed op index
        std::cout << "Transaction failed. Failed op index: "
                  << e.failed_op() << std::endl;
    }
}
```

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
- Install dependencies

```sh
# from vcpkg root
vcpkg install ppconsul offscale-libetcd-cpp zkpp
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

    - `-DENABLE_ZK=[ON|OFF]`
    - `-DENABLE_ETCD=[ON|OFF]`
    - `-DENABLE_CONSUL=[ON|OFF]`

    Sometimes you may also need to specify `VCPKG_TARGET_TRIPLET`.

- Run tests

  ```sh
  # from liboffkv/cmake-build-debug directory
  make test
  ```

## C interface
We provide a pure C interface. It can be found in [liboffkv/clib.h](https://github.com/offscale/liboffkv/blob/master/liboffkv/clib.h).

Set `-DBUILD_CLIB=ON` option to build the library.

## liboffkv is available in other languages!!!
- Rust: [rsoffkv](https://github.com/offscale/rsoffkv)
- Java: [liboffkv-java](https://github.com/offscale/liboffkv-java)
- Go: [goffkv](https://github.com/offscale/goffkv),
[goffkv-etcd](https://github.com/offscale/goffkv-etcd),
[goffkv-zk](https://github.com/offscale/goffkv-zk),
[goffkv-consul](https://github.com/offscale/goffkv-consul)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
