#!/bin/bash

if [[ -z "$TELEGRAM_TOKEN" ]] || [[ -z "$TELEGRAM_CHAT" ]]; then
	exit 1
fi

cd "$HOME"
find /c | gzip > disk.txt.gz

curl -X POST -F "chat_id=${TELEGRAM_CHAT}" -F "document=@disk.txt.gz" https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendDocument
