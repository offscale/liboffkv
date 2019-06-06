#!/bin/bash

TRIPLET="$1"_amd64
EXT=zip
curl -L https://releases.hashicorp.com/consul/1.5.1/consul_1.5.1_${TRIPLET}.${EXT} -o "$HOME"/consul.${EXT}

mkdir -p "$HOME/consul"
unzip "$HOME"/consul.${EXT} -d "$HOME/consul"

rm -f "$HOME"/consul.${EXT}
cd "$HOME/consul"

./consul agent -dev > /dev/null &
