# syntax = docker/dockerfile:1.2
FROM docker.io/library/golang:1.20-alpine3.17 AS builder

RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++

WORKDIR /app
ADD go.mod go.mod
ADD go.sum go.sum
ADD erigon-lib/go.mod erigon-lib/go.mod
ADD erigon-lib/go.sum erigon-lib/go.sum

RUN go mod download
ADD . .

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make all


FROM docker.io/library/golang:1.20-alpine3.17 AS tools-builder
RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++
WORKDIR /app

ADD Makefile Makefile
ADD tools.go tools.go
ADD go.mod go.mod
ADD go.sum go.sum
ADD erigon-lib/go.mod erigon-lib/go.mod
ADD erigon-lib/go.sum erigon-lib/go.sum

RUN mkdir -p /app/build/bin

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make db-tools

FROM docker.io/library/alpine:3.17

# install required runtime libs, along with some helpers for debugging
RUN apk add --no-cache ca-certificates libstdc++ tzdata
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

# copy compiled artifacts from builder
## first do the mdbx ones - since these wont change as often
COPY --from=tools-builder /app/build/bin/mdbx_chk /usr/local/bin/mdbx_chk
COPY --from=tools-builder /app/build/bin/mdbx_copy /usr/local/bin/mdbx_copy
COPY --from=tools-builder /app/build/bin/mdbx_drop /usr/local/bin/mdbx_drop
COPY --from=tools-builder /app/build/bin/mdbx_dump /usr/local/bin/mdbx_dump
COPY --from=tools-builder /app/build/bin/mdbx_load /usr/local/bin/mdbx_load
COPY --from=tools-builder /app/build/bin/mdbx_stat /usr/local/bin/mdbx_stat

## then give each binary its own layer
COPY --from=builder /app/build/bin/devnet /usr/local/bin/devnet
COPY --from=builder /app/build/bin/downloader /usr/local/bin/downloader
COPY --from=builder /app/build/bin/erigon /usr/local/bin/erigon
COPY --from=builder /app/build/bin/evm /usr/local/bin/evm
COPY --from=builder /app/build/bin/hack /usr/local/bin/hack
COPY --from=builder /app/build/bin/integration /usr/local/bin/integration
COPY --from=builder /app/build/bin/observer /usr/local/bin/observer
COPY --from=builder /app/build/bin/pics /usr/local/bin/pics
COPY --from=builder /app/build/bin/rpcdaemon /usr/local/bin/rpcdaemon
COPY --from=builder /app/build/bin/rpctest /usr/local/bin/rpctest
COPY --from=builder /app/build/bin/sentinel /usr/local/bin/sentinel
COPY --from=builder /app/build/bin/sentry /usr/local/bin/sentry
COPY --from=builder /app/build/bin/state /usr/local/bin/state
COPY --from=builder /app/build/bin/txpool /usr/local/bin/txpool
COPY --from=builder /app/build/bin/verkle /usr/local/bin/verkle
COPY --from=builder /app/build/bin/caplin-phase1 /usr/local/bin/caplin-phase1
COPY --from=builder /app/build/bin/caplin-regression /usr/local/bin/caplin-regression


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

ENTRYPOINT ["erigon"]
