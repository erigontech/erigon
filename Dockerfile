FROM golang:1.15-alpine as builder

RUN apk add --no-cache make gcc musl-dev linux-headers git

WORKDIR /app

# next 2 lines helping utilize docker cache
COPY go.mod go.sum ./
RUN go mod download

ADD . .
RUN make all

FROM alpine:3

RUN apk add --no-cache ca-certificates
COPY --from=builder /app/build/bin/* /usr/local/bin/

EXPOSE 8545 8546 8547 30303 30303/udp 8080 9090 6060
