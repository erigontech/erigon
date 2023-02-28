#!/bin/bash
set -xe
DOCKER_BUILDKIT=1 docker build -t us-west1-docker.pkg.dev/sentio-352722/sentio/erigon:dev  . && docker push us-west1-docker.pkg.dev/sentio-352722/sentio/erigon:dev
