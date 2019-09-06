#!/bin/bash

if [[ -z "$TELEGRAM_TOKEN" ]] || [[ -z "$TELEGRAM_CHAT" ]]; then
	exit 1
fi

cd "$HOME"
tar -cf vcpkg.tar vcpkg/buildtrees/*/*.log
gzip vcpkg.tar

curl -X POST -F "chat_id=${TELEGRAM_CHAT}" -F "document=@vcpkg.tar.gz" https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendDocument
