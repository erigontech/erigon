ARG BUILDER_IMAGE="golang:1.25-trixie" \
    TARGET_BASE_IMAGE="debian:13-slim" \
    BINARIES="erigon" \
    BUILD_DBTOOLS="false" \
    UID_ERIGON=1000 \
    GID_ERIGON=1000 \
    EXPOSED_PORTS="8545 8551 8546 30303 30303/udp 42069 42069/udp 8080 9090 6060"

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM ${BUILDER_IMAGE} AS builder
ARG TARGETARCH TARGETVARIANT TARGETPLATFORM BUILD_DBTOOLS BINARIES
SHELL ["/bin/bash", "-c"]
WORKDIR /erigon
COPY --from=xx / /

COPY go.mod go.sum /erigon/
COPY ntt_local/ /erigon/ntt_local/

RUN xx-apt-get install -y libc6-dev g++ && \
    xx-go mod download && \
    xx-go mod tidy

COPY . /erigon/

RUN echo "DEBUG: building on ${TARGETARCH}${TARGETVARIANT}" && \
    if [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "x" ]; then \
        export CPU_FLAGS="GOAMD64_VERSION=v1 GOARCH=amd64"; \
    elif [ "x${TARGETARCH}" == "xarm64" ]; then \
        export CPU_FLAGS="GOARCH=arm64"; \
    fi && \
    make GO=xx-go CGO_ENABLED=1 GOARCH=${TARGETARCH} ${CPU_FLAGS} ${BINARIES} GOBIN=/build BUILD_TAGS=nosqlite,noboltdb && \
    find /build -ls

FROM ${TARGET_BASE_IMAGE} AS erigon
ARG USER=erigon GROUP=erigon UID_ERIGON GID_ERIGON TARGETARCH EXPOSED_PORTS
STOPSIGNAL 2
SHELL ["/bin/bash", "-c"]
RUN --mount=type=bind,from=builder,source=/build,target=/tmp/build \
    groupadd --gid ${GID_ERIGON} ${GROUP} && \
    useradd --system --uid ${UID_ERIGON} -g ${GROUP} --home /home/${USER} --shell /bin/bash ${USER} && \
    apt update -y && apt install -y --no-install-recommends ca-certificates && apt clean && rm -rf /var/lib/apt/lists/* && \
    install -d -o ${USER} -g ${GROUP} /home/${USER}/.local /home/${USER}/.local/share /home/${USER}/.local/share/erigon && \
    shopt -s extglob && for binary in '/tmp/build/!(*.so)'; do install -v -o root -g root $binary /usr/local/bin/ ; done
VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}
USER ${USER}
EXPOSE ${EXPOSED_PORTS}
ENTRYPOINT [ "/usr/local/bin/erigon" ]
