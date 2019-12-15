#!/usr/bin/env bash

set -e

EXT=tar.gz
curl -L https://archive.apache.org/dist/zookeeper/zookeeper-3.5.5/apache-zookeeper-3.5.5-bin.${EXT} -o "$HOME"/zk.${EXT}

mkdir -p "$HOME/zk"
tar xzf "$HOME"/zk.${EXT} -C "$HOME/zk" --strip-components=1

rm -f "$HOME"/zk.${EXT}
cd "$HOME/zk"

echo "tickTime=2000
dataDir=$HOME/zk/data_
clientPort=2181" > "$HOME/zk/conf/zoo.cfg"

bin/zkServer.sh start
