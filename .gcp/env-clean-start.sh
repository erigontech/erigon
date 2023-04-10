#!/usr/bin/env sh
# Recreate environment - full reset and start

SCRIPT_DIR=$(realpath $(dirname "$0"))
. $SCRIPT_DIR/config.env
export TERM=xterm-mono

set -e

mkdir -p ~/db
rm -rf ~/db/*

docker compose -f ~/scripts/docker-compose.yaml stop
docker compose -f ~/scripts/docker-compose.yaml down
docker volume rm -f scripts_db-volume
echo "Compose images pull started"
docker compose -f ~/scripts/docker-compose.yaml pull --quiet
echo "Compose images successfuly pulled"
# docker volume rm scripts_db-volume
docker compose -f ~/scripts/docker-compose.yaml create

docker compose -f ~/scripts/docker-compose.yaml start > /dev/null
if [ $? -eq 0 ]; then 
  echo "COMPOSE STARTED" 
else 
  echo "Can't properly start compose /(o_O)\\" >&2 
fi
