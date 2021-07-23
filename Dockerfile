FROM docker.io/library/golang:1.16-alpine3.13 as builder

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++

WORKDIR /app
ADD . .

RUN make erigon rpcdaemon integration sentry

FROM docker.io/library/alpine:3.13

# Directories spec https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
RUN mkdir -p /home/erigon/.local/share
VOLUME /home/erigon/.local/share

RUN apk add --no-cache ca-certificates libgcc libstdc++ tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

WORKDIR /home/erigon/.local/share

RUN adduser -H -u 1000 -g 1000 -D erigon
RUN chown -R erigon:erigon /home/erigon
USER erigon

EXPOSE 8545 8546 30303 30303/udp 30304 30304/udp 8080 9090 6060
