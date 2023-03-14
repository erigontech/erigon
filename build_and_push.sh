#!/bin/bash
set -xe
IMAGE_TAG="${IMAGE_TAG:-dev}"
docker buildx build -t us-west1-docker.pkg.dev/sentio-352722/sentio/erigon:$IMAGE_TAG . && docker push us-west1-docker.pkg.dev/sentio-352722/sentio/erigon:$IMAGE_TAG
