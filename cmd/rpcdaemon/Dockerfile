FROM golang:1.14-alpine as builder

RUN apk add --no-cache make gcc musl-dev linux-headers git

# expecting build context: whole turbo-geth repo
# next 2 lines helping utilize docker cache
COPY go.mod go.sum /app/
RUN cd /app && go mod download

ADD . /app
RUN cd /app && make rpcdaemon

# Pull Geth into a second stage deploy alpine container
FROM alpine:3

RUN apk add --no-cache ca-certificates
COPY --from=builder /app/build/bin/rpcdaemon /usr/local/bin/

EXPOSE 8545
ENTRYPOINT ["rpcdaemon"]
