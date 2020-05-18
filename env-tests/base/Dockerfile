FROM debian:buster-slim

ENV VCPKG_ROOT=/test/vcpkg CXX=/usr/bin/g++-8

RUN apt-get update && apt-get install -y g++-8 make cmake git curl unzip tar iptables
RUN mkdir /test
RUN git clone --single-branch --branch=master --depth=1 'https://github.com/Microsoft/vcpkg' "$VCPKG_ROOT"

WORKDIR "$VCPKG_ROOT"
RUN ./bootstrap-vcpkg.sh -disableMetrics
RUN ./vcpkg install gtest ppconsul zkpp offscale-libetcd-cpp && rm -rf buildtrees/*
WORKDIR /test
