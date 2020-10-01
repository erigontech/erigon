FROM golang:1.15 as builder

WORKDIR /app

# next 2 lines helping utilize docker cache
COPY go.mod go.sum ./
RUN go mod download

ADD . .
RUN make all

FROM ubuntu:18.04

COPY --from=builder /app/build/bin/* /usr/local/bin/

EXPOSE 8545 8546 8547 30303 30303/udp 8080 9090 6060
