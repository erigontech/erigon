#!/usr/bin/env sh
SCRIPT_DIR=$(realpath $(dirname "$0"))
. $SCRIPT_DIR/config.env

set -e

mkdir -p ~/db
rm -rf ~/db/*

docker-compose -f ~/scripts/docker-compose.yaml logs -f

