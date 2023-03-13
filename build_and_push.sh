#!/bin/bash
set -xe
rm -rf erigon-lib
cp -R ../erigon-lib ./
IMAGE_TAG="${IMAGE_TAG:-dev}"
DOCKER_BUILDKIT=1 docker build -t us-west1-docker.pkg.dev/sentio-352722/sentio/erigon:$IMAGE_TAG . && docker push us-west1-docker.pkg.dev/sentio-352722/sentio/erigon:$IMAGE_TAG
rm -rf erigon-lib
