testenv
=======

testenv is a docker-based environment manager for advanced testing.

## Why testenv?

Most cases can be covered with unit tests assuming the services are started locally. However, it is also worth testing not only how the library handles its own actions but how it responds to external conditions like connection loss.

It means that tests must not only interact with the library but affect the environment. You probably don't want to break the networking of your workstation. Here testenv comes in.

## Components

testenv consists of

- third-party service images with ZooKeeper, Consul and etcd
- base image with vcpkg and precompiled liboffkv dependencies
- bash script to govern them all

As you work with testenv, more components appear:

- testing images
- service containers
- testing containers

## Workflow

```sh
# print usage info
testenv 

# download service images and build base image
# it will take some time
testenv prepare 

# start 3 clusters 3 nodes each
testenv run -nzk:3 -netcd:3 -nconsul:3

# print running testenv entities
testenv status

# run tests against the clusters
# see details below
testenv test -d /path/to/liboffkv -g -DBUILD_ENV_TESTS=ON -r tests/conn_test

# stop all running testenv entities
testenv stop

# remove all testenv containers and testing images
testenv clear

# remove all testenv containers and images
testenv clear -a
docker system prune
```

`testenv test` will actually

- Build an image with the contents from directory specified with `-d`. The directory may not contain Dockerfile. testenv provides the default one. However, custom Dockerfile can be used for more precise control.
- Inside the container it normally executes
  - `cmake -DCMAKE_TOOLCHAIN_FILE=/p/to/vcpkg.cmake .`; additional arguments can be passed with `-g`.
  - `cmake --build .`; ; additional arguments can be passed with `-b`, e.g. `-g --target test_etcd`.
  - This behavior can be overridden by passing `generate_cmd=cmd`, `build_cmd=cmd`.
- Form a list of addresses for each service.
- Sequentially run containers with the image for each service in interactive mode. The container will receive the command passed via `-r` concatenated with the list of addresses.

## Introducing conditions

Default base image for tests has `iptables` bundled. Testing are run with `--cap-add NET_ADMIN`. Thus all the power of `iptables` can be used to manipulate networking directly from the tests.