#!/usr/bin/env sh
# Reset DB, restart env with the same docker images

SCRIPT_DIR=$(realpath $(dirname "$0"))
. $SCRIPT_DIR/config.env
export TERM=xterm-mono

set -e

docker compose -f ~/scripts/docker-compose.yaml stop

mkdir -p ~/db
rm -rf ~/db/*

docker compose -f ~/scripts/docker-compose.yaml down
docker volume rm -f scripts_db-volume
docker compose -f ~/scripts/docker-compose.yaml create

docker compose -f ~/scripts/docker-compose.yaml start > /dev/null

if [ $? -eq 0 ]; then 
  echo "COMPOSE STARTED" 
else 
  echo "Can't properly start compose /(o_O)\\" >&2 
fi
