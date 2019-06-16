#!/bin/bash

ROOT="$1"
N1=$(ls -1 "$ROOT/downloads/tools/" | grep cmake)
N2=$(ls -1 "$ROOT/downloads/tools/$N1" | grep cmake)

echo "$ROOT/downloads/tools/$N1/$N2/bin"
