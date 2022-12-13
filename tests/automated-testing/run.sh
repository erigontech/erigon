#!/bin/bash

function stopContainers () {
    # stop containers
    echo "stopping containers..."
    docker-compose --profile=first down -v --remove-orphans
    docker-compose --profile=second down -v --remove-orphans
}

ORIGINAL_DIR=$(pwd)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR" || exit

#export DOCKER_UID=1000
#export DOCKER_GID=1000

# set GITHUB_SHA
if [ -z "$GITHUB_SHA" ]; then
    export GITHUB_SHA=local
fi
echo "GITHUB_SHA=$GITHUB_SHA"

# set ERIGON_TAG
if [ -z "$ERIGON_TAG" ]; then
    export ERIGON_TAG=ci-$GITHUB_SHA
fi
echo "ERIGON_TAG=$ERIGON_TAG"

# set BUILD_ERIGON
if [ -z "$BUILD_ERIGON" ]; then
    export BUILD_ERIGON=0
fi
echo "BUILD_ERIGON=$BUILD_ERIGON"

if [ "$BUILD_ERIGON" = 1 ] ; then
    echo "building erigon..."
    cd ../../ && DOCKER_TAG=thorax/erigon:$ERIGON_TAG  DOCKER_UID=$(id -u) DOCKER_GID=$(id -g) make docker
fi

# move back to the script directory
cd "$SCRIPT_DIR" || exit

# pull container images
echo "pulling container images..."
docker compose pull

# run node 1
echo "starting node 1..."
docker-compose --profile=first up -d --force-recreate --remove-orphans

# wait for node 1 to start up
echo "waiting for node 1 to start up..."
sleep 10

# run node 2
echo "starting node 2..."
export ENODE=$(./scripts/enode.sh)
docker-compose --profile=second up -d --force-recreate --remove-orphans

# wait for node 2 to start up
echo "waiting for node 2 to start up..."
sleep 10

# run tests!
echo "running tests..."
docker compose run --rm tests || { echo 'tests failed'; stopContainers; exit 1; }

stopContainers

cd "$ORIGINAL_DIR" || exit