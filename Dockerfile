FROM golang:1.16-alpine3.13 as builder

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++ openssh

WORKDIR /app
ADD . .

RUN git config advice.detachedHead false
RUN git fetch --tags

RUN make all

FROM alpine:3.13

ARG USER=erigon
ARG UID=10001

# See https://stackoverflow.com/a/55757473/12429735RUN
RUN adduser \
    --disabled-password \
    --gecos "" \
    --shell "/sbin/nologin" \
    --uid "${UID}" \
    "${USER}"

RUN mkdir -p /home/erigon && chown ${USER}:${USER} /home/erigon

RUN apk add --no-cache ca-certificates libgcc libstdc++ tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

USER ${USER}

EXPOSE 8545 8546 30303 30303/udp 30304 30304/udp 8080 9090 6060
