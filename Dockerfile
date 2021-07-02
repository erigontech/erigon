FROM golang:1.16-alpine3.13 as builder

# Here only to avoid build-time errors
ARG DOCKER_TAG

ARG BUILD_TARGET

ARG git_commit
ENV GIT_COMMIT=$git_commit

ARG git_branch
ENV GIT_BRANCH=$git_branch

ARG git_tag
ENV GIT_TAG=$git_tag

RUN apk --no-cache add make gcc g++ linux-headers git bash ca-certificates libgcc libstdc++

WORKDIR /app

RUN git clone --recurse-submodules -j8 https://github.com/ledgerwatch/erigon.git .
RUN git config advice.detachedHead false
RUN git fetch --all --tags
RUN git checkout ${BUILD_TARGET}

RUN go mod download
RUN go build ./cmd/erigon

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

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/build/bin/* /usr/local/bin/

USER ${USER}

EXPOSE 8545 8546 30303 30303/udp 30304 30304/udp 8080 9090 6060
