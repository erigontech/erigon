#!/usr/bin/env sh
SCRIPT_DIR=$(realpath $(dirname "$0"))
. $SCRIPT_DIR/config.env

set -e

mkdir -p ~/scripts
rm -rf ~/scripts/*
gsutil cp gs://$BUCKET_PATH/* ~/scripts/
chmod -R +x ~/scripts

echo "SCRIPTS UPDATED"