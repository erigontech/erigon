FROM docker.io/library/golang:1.16-alpine3.13 as builder

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++

WORKDIR /app
ADD . .

RUN make erigon rpcdaemon integration sentry

FROM docker.io/library/alpine:3.13

RUN mkdir -p /var/lib/erigon
VOLUME /var/lib/erigon

RUN apk add --no-cache ca-certificates libgcc libstdc++ tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

WORKDIR /var/lib/erigon

EXPOSE 8545 8546 30303 30303/udp 30304 30304/udp 8080 9090 6060
