FROM golang:1.13-alpine as builder

RUN apk add --no-cache make gcc musl-dev linux-headers git

# next 2 lines helping utilize docker cache
COPY go.mod go.sum /go-ethereum/
RUN cd /go-ethereum && go mod download

ADD . /go-ethereum
RUN cd /go-ethereum && make restapi

# Pull Geth into a second stage deploy alpine container
FROM alpine:latest

RUN apk add --no-cache ca-certificates
COPY --from=builder /go-ethereum/build/bin/restapi /usr/local/bin/

EXPOSE 8080
ENTRYPOINT ["restapi"]
