liboffkv
========
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/offscale/liboffkv.svg?branch=master)](https://travis-ci.org/offscale/liboffkv)

#### The library is designed to provide a uniform interface for three distributed KV storages: etcd, ZooKeeper and Consul.

The services have similar but different data models, so we outlined the common features. 

In our implementation, keys form a ZK-like hierarchy. Each key has a version that is uint64 number greater than 0. Newly created keys have version 1. Current version is returned with other data by the most of operations. All the operations supported are listed below. 

Our library has only an asynchronous interface, but if you want a synchronous one, just call `get()` on the returned futures.

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
          preceding entry does not exist.</td>
    </tr>
    <tr>
    	<td>set</td>
        <td><b>key:</b> string<br>
        	<b>value:</b> char[]
		</td>
        <td>Assigns the value.<br>
        	<u>Creates</u> the key if it doesn’t exist.<br>
            Throws an exception if preceding entry does not exist.
        </td>
    </tr>
    <tr>
    	<td>cas</td>
        <td><b>key:</b> string<br>
        	<b>value:</b> char[]<br>
            <b>version:</b> uint64 (=0) -- <i>expected version of the key</i>
		</td>
        <td>
        	Compare and set operation.<br>
			If the key does not exist and version equals 0 creates it.<br>
            Throws an exception if preceding entry does not exist.<br>
			If the key exists and its version equals to specified one updates the value.
			Otherwise does nothing.
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
            If <b>watch</b> is true, returns a future that will be waiting til<br>
            the value is changed (see an example below).
        </td>
    </tr>
    <tr>
    	<td>exists</td>
        <td><b>key:</b> string<br>
        	<b>watch:</b> bool (=false) -- <i>start watching for removal or creation of the key</i>
        </td>
        <td>
        	Checks if the key exists.<br>
            If <b>watch</b> is true, returns a future that will be waiting for the key to be erased or created.
        </td>
    </tr>
    <tr>
    	<td>get_children</td>
        <td><b>key:</b> string<br>
        	<b>watch:</b> bool (=false)<br>
        </td>
        <td>
        Returns a list of the key's <u>direct</u> children.<br>
        Throws an exception if the key does not exist.<br>
        If <b>watch</b> is true, returns a future that will be waiting for any changes among the key's children.
        </td>
    </tr>
    <tr>
    	<td>erase</td>
        <td><b>key:</b> string<br>
        	<b>version:</b> uint64 (=0)<br>
            <b>watch:</b> bool (=false)</td>
    	<td>
        Erases the key and <u>all its descendants</u> if given <b>version</b> equals to the current key's version,<br>
        or does it unconditionally if <b>version</b> is 0.<br>
        Throws an exception if the key does not exist.<br>
    </tr>
    <tr>
    	<td>commit</td>
        <td><b>transaction:</b> Transaction</td>
        <td>Commits transaction (see transactions API below).
    </tr>
  </tbody>
</table>

### Transactions
Transactions consist of 4 operations: create, set, erase, check. Their descriptions can be found below. At the moment set has different behavior in comparison to ordinary set: when used in transaction, set does not create key if it does not exist. Besides using any of the operations you cannot assign watch. Transaction body is separated into two blocks: firstly you should write all required checks and then the sequence of other operations <i>(see an example below)</i>. As a result you get a list of new versions for all the keys involved in set operations.
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
        	<b>version:</b> uint64 (=0)<br>
    	<td>
        Erases the key and <u>all its descendants</u><br>
        if given <b>version</b> equals to the current key's version,<br>
        or does it unconditionally if <b>version</b> is 0.<br>
        Rolls back if the key does not exist.<br>
    </tr>
    <tr>
    	<td>check</td>
        <td><b>key:</b> string<br>
        	<b>version:</b> uint64
        </td>
        <td>Checks if given key has specified version<br>
        or only checks if it exists if <b>version</b> is 0
    </tr>
  </tbody>
</table>


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

