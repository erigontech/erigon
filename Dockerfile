FROM golang:1.15-alpine as builder

RUN apk add --no-cache make gcc musl-dev linux-headers git

WORKDIR /app

# next 2 lines helping utilize docker cache
COPY go.mod go.sum ./
RUN go mod download

# https://github.com/valyala/gozstd/issues/20#issuecomment-557499034
RUN GOZSTD_VER=$(cat go.mod | fgrep github.com/valyala/gozstd | awk '{print $NF}'); cd ${GOPATH}/pkg/mod/github.com/valyala/gozstd@${GOZSTD_VER}; if [[ ! -f _rebuilt ]]; then chmod -R +w .; make -j8 clean; make -j8 libzstd.a; touch _rebuilt; fi;

ADD . .
RUN make all

FROM alpine:3

RUN apk add --no-cache ca-certificates
COPY --from=builder /app/build/bin/* /usr/local/bin/

EXPOSE 8545 8546 30303 30303/udp 8080 9090 6060
