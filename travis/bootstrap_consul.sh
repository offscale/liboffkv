#!/usr/bin/env bash

set -e

TRIPLET="$1"_amd64
EXT=zip
curl -L https://releases.hashicorp.com/consul/1.5.1/consul_1.5.1_${TRIPLET}.${EXT} -o ~/consul.${EXT}

mkdir -p ~/consul
unzip -q ~/consul.${EXT} -d ~/consul

rm -f ~/consul.${EXT}
cd ~/consul

./consul agent -dev > /dev/null &
