#!/usr/bin/env bash

set -e

TRIPLET="$1"-amd64
EXT=$2
curl -L https://github.com/etcd-io/etcd/releases/download/v3.3.13/etcd-v3.3.13-${TRIPLET}.${EXT} -o "$HOME"/etcd.${EXT}

mkdir -p "$HOME/etcd"
if [[ "$EXT" == "tar.gz" ]]; then
	tar xzf "$HOME"/etcd.${EXT} -C "$HOME/etcd" --strip-components=1
elif [[ "$EXT" == "zip" ]]; then
	unzip "$HOME"/etcd.${EXT} -d "$HOME/etcd"
	f=("$HOME/etcd"/*) && mv "$HOME/etcd"/*/* "$HOME/etcd" && rmdir "${f[@]}"
else
	exit 1
fi

rm -f "$HOME"/etcd.${EXT}
cd "$HOME/etcd"

mkdir _data
./etcd --data-dir "$PWD/_data" > /dev/null &
