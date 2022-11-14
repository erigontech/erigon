# syntax = docker/dockerfile:1.2
FROM docker.io/library/golang:1.19-alpine3.16 AS builder

RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++

WORKDIR /app
ADD . .

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make all db-tools

FROM docker.io/library/alpine:3.16

RUN apk add --no-cache ca-certificates curl libstdc++ jq tzdata
# copy compiled artifacts from builder
COPY --from=builder /app/build/bin/* /usr/local/bin/

# Setup user and group
#
# from the perspective of the container, uid=1000, gid=1000 is a sensible choice
# (mimicking Ubuntu Server), but if caller creates a .env (example in repo root),
# these defaults will get overridden when make calls docker-compose
ARG UID=1000
ARG GID=1000
RUN adduser -D -u $UID -g $GID erigon
USER erigon
RUN mkdir -p ~/.local/share/erigon

EXPOSE 8545 \
       8551 \
       8546 \
       30303 \
       30303/udp \
       42069 \
       42069/udp \
       8080 \
       9090 \
       6060

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.description="Erigon Ethereum Client" \
      org.label-schema.name="Erigon" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.url="https://torquem.ch" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/ledgerwatch/erigon.git" \
      org.label-schema.vendor="Torquem" \
      org.label-schema.version=$VERSION
