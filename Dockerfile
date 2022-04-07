# syntax=docker/dockerfile:1
FROM docker.io/library/golang:1.17-alpine3.15 AS builder

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++

WORKDIR /app
ADD . .

# expect that host run `git submodule update --init`
RUN make erigon rpcdaemon integration sentry txpool downloader hack db-tools

FROM docker.io/library/alpine:3.15

RUN apk add --no-cache ca-certificates libgcc libstdc++ tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

RUN adduser -H -u 1000 -g 1000 -D erigon
RUN mkdir -p /home/erigon
RUN mkdir -p /home/erigon/.local/share/erigon
RUN chown -R erigon:erigon /home/erigon

USER erigon

EXPOSE 8545 8550 8551 8546 30303 30303/udp 30304 30304/udp 8080 9090 6060

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
ARG BUILD_DATE
ARG VCS_REF
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="Erigon" \
      org.label-schema.description="Erigon Ethereum Client" \
      org.label-schema.url="https://torquem.ch" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/ledgerwatch/erigon.git" \
      org.label-schema.vendor="Torquem" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"
