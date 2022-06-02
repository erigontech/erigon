# syntax = docker/dockerfile:1.2
FROM docker.io/library/golang:1.18-alpine3.15 AS builder

RUN apk --no-cache add build-base linux-headers git bash ca-certificates libstdc++

WORKDIR /app
ADD . .

RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    make all db-tools

FROM docker.io/library/alpine:3.15

RUN apk add --no-cache ca-certificates libstdc++ tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

RUN adduser -H -u 1000 -g 1000 -D erigon
RUN mkdir -p /home/erigon
RUN mkdir -p /home/erigon/.local/share/erigon
RUN chown -R erigon:erigon /home/erigon

USER erigon

EXPOSE 8545 8551 8546 30303 30303/udp 42069 42069/udp 8080 9090 6060

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="Erigon" \
      org.label-schema.description="Erigon Ethereum Client" \
      org.label-schema.url="https://torquem.ch" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/ledgerwatch/erigon.git" \
      org.label-schema.vendor="Torquem" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"
