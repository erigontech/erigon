FROM docker.io/library/golang:1.16-alpine3.13 as builder

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++

WORKDIR /app
ADD . .

RUN make erigon rpcdaemon integration sentry

FROM docker.io/library/alpine:3.13

RUN apk add --no-cache ca-certificates libgcc libstdc++ tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

RUN adduser -H -u 1000 -g 1000 -D erigon
USER erigon

# Directories spec https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
RUN mkdir -p /home/erigon
RUN mkdir -p /home/erigon/.local/share
RUN chown -R erigon:erigon /home/erigon
VOLUME /home/erigon/.local/share
WORKDIR /home/erigon/.local/share

EXPOSE 8545 8546 30303 30303/udp 30304 30304/udp 8080 9090 6060
