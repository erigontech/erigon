#!/usr/bin/env sh
SCRIPT_DIR=$(realpath $(dirname "$0"))
. $SCRIPT_DIR/config.env
export TERM=xterm-mono

set -e

docker compose -f ~/scripts/docker-compose.yaml rm -fs
echo "Compose images pull started"
docker compose -f ~/scripts/docker-compose.yaml pull --quiet
echo "Compose images successfuly pulled"
docker compose -f ~/scripts/docker-compose.yaml create
docker compose -f ~/scripts/docker-compose.yaml start > /dev/null

if [ $? -eq 0 ]; then 
  echo "COMPOSE STARTED" 
else 
  echo "Can't properly start compose /(o_O)\\" >&2 
fi
