# syntax = docker/dockerfile:1.2
FROM docker.io/library/golang:1.24.3-alpine3.22 AS builder

RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++ gmp-dev

WORKDIR /app
ADD go.mod go.mod
ADD go.sum go.sum
ADD erigon-lib/go.mod erigon-lib/go.mod
ADD erigon-lib/go.sum erigon-lib/go.sum
ADD erigon-db/go.mod erigon-db/go.mod
ADD erigon-db/go.sum erigon-db/go.sum
ADD p2p/go.mod p2p/go.mod
ADD p2p/go.sum p2p/go.sum

RUN go mod download
ADD . .

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make BUILD_TAGS=nosqlite,noboltdb,nosilkworm rpcdaemon

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make BUILD_TAGS=nosqlite,noboltdb,nosilkworm erigon


FROM docker.io/library/alpine:3.22

# install required runtime libs, along with some helpers for debugging
RUN apk add --no-cache ca-certificates libstdc++ tzdata gmp
RUN apk add --no-cache curl jq bind-tools

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

## then give each binary its own layer
COPY --from=builder /app/build/bin/rpcdaemon /usr/local/bin/rpcdaemon
COPY --from=builder /app/build/bin/erigon /usr/local/bin/erigon

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
      org.label-schema.url="https://erigon.tech" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/erigontech/erigon.git" \
      org.label-schema.vendor="Erigon" \
      org.label-schema.version=$VERSION

ENTRYPOINT ["erigon"]
