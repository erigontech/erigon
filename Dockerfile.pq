## Patched Dockerfile for Erigon with PQ precompiles.
## Adds a Rust build stage to compile libeth_ntt from source,
## since the precompiled .a files in the Go module may be outdated.

ARG BUILDER_IMAGE="golang:1.25-trixie" \
    TARGET_BASE_IMAGE="debian:13-slim" \
    BINARIES="erigon" \
    BUILD_DBTOOLS="false" \
    BUILD_DATE="Not defined" \
    VCS_REF="Not defined" \
    UID_ERIGON=1000 \
    GID_ERIGON=1000 \
    EXPOSED_PORTS="8545 \
       8551 \
       8546 \
       30303 \
       30303/udp \
       42069 \
       42069/udp \
       8080 \
       9090 \
       6060"

## Use xx - Dockerfile cross-compilation helpers
FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

### Rust builder: compile libeth_ntt.a from pq-eth-precompiles source
FROM --platform=$BUILDPLATFORM rust:1.85-slim-bookworm AS rust-builder
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc-aarch64-linux-gnu gcc-x86-64-linux-gnu \
    libc6-dev-arm64-cross libc6-dev-amd64-cross \
    && rm -rf /var/lib/apt/lists/*

# Add Rust targets
RUN rustup target add aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu

WORKDIR /pq-precompiles
COPY ntt_local/rust/ /pq-precompiles/

# Build for the target architecture. cdylib link may fail cross-compile but staticlib is still produced.
RUN if [ "${TARGETARCH}" = "arm64" ]; then \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc && \
        cargo build --release --target aarch64-unknown-linux-gnu || true && \
        mkdir -p /output && \
        cp target/aarch64-unknown-linux-gnu/release/libpq_eth_precompiles.a /output/libeth_ntt.a && \
        nm /output/libeth_ntt.a | grep -c "T.*eth_ntt" && echo "FFI symbols OK"; \
    else \
        export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc && \
        cargo build --release --target x86_64-unknown-linux-gnu || true && \
        mkdir -p /output && \
        cp target/x86_64-unknown-linux-gnu/release/libpq_eth_precompiles.a /output/libeth_ntt.a && \
        nm /output/libeth_ntt.a | grep -c "T.*eth_ntt" && echo "FFI symbols OK"; \
    fi

### Erigon Builder section:
FROM --platform=$BUILDPLATFORM ${BUILDER_IMAGE} AS builder
ARG TARGETARCH \
    TARGETVARIANT \
    TARGETPLATFORM \
    BUILD_DBTOOLS \
    BINARIES

SHELL ["/bin/bash", "-c"]

WORKDIR /erigon

## Copy all content of helpers:
COPY --from=xx / /

COPY go.mod go.sum /erigon/
COPY ntt_local/ /erigon/ntt_local/

# Replace the precompiled library with the freshly-built one
COPY --from=rust-builder /output/libeth_ntt.a /fresh_lib/libeth_ntt.a
RUN ARCH_DIR="" && \
    if [ "${TARGETARCH}" = "arm64" ]; then ARCH_DIR="linux_arm64"; \
    else ARCH_DIR="linux_amd64"; fi && \
    cp /fresh_lib/libeth_ntt.a /erigon/ntt_local/ntt/lib/${ARCH_DIR}/libeth_ntt.a

## Make sure required dependencies are installed (some packages required only for arm64):
RUN xx-apt-get install -y libc6-dev g++ && \
    xx-go mod download && \
    xx-go mod tidy

COPY . /erigon/

# Replace the library again after COPY . overwrites ntt_local/
RUN ARCH_DIR="" && \
    if [ "${TARGETARCH}" = "arm64" ]; then ARCH_DIR="linux_arm64"; \
    else ARCH_DIR="linux_amd64"; fi && \
    cp /fresh_lib/libeth_ntt.a /erigon/ntt_local/ntt/lib/${ARCH_DIR}/libeth_ntt.a

RUN echo "DEBUG: building on ${TARGETARCH}${TARGETVARIANT}" && \
    if [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "x" ]; then \
        echo "DEBUG: detected architecture AMD64v1"; \
        export CPU_FLAGS="GOAMD64_VERSION=v1 GOARCH=amd64"; \
    elif [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "xv2" ]; then \
        echo "DEBUG: detected architecture AMD64v2"; \
        export CPU_FLAGS="GOAMD64_VERSION=v2 GOARCH=amd64"; \
    elif [ "x${TARGETARCH}" == "xarm64" ]; then \
        echo "DEBUG: detected architecture ARM64"; \
        export CPU_FLAGS="GOARCH=arm64"; \
    fi && \
    echo "DEBUG: cmd - make ${CPU_FLAGS} ${BINARIES} GOBIN=/build ." && \
    make GO=xx-go CGO_ENABLED=1 GOARCH=${TARGETARCH} ${CPU_FLAGS} ${BINARIES} GOBIN=/build BUILD_TAGS=nosqlite,noboltdb && \
    if [ "x${BUILD_DBTOOLS}" == "xtrue" ]; then \
        echo "Building db-tools:"; \
        make GO=xx-go CGO_ENABLED=1 GOBIN=/build db-tools; \
    fi && \
    find /build -ls

### End of builder section


### Erigon Target section:
FROM ${TARGET_BASE_IMAGE} AS erigon
ARG USER=erigon \
    GROUP=erigon \
    UID_ERIGON \
    GID_ERIGON \
    TARGETARCH \
    TARGET_BASE_IMAGE \
    EXPOSED_PORTS \
    BUILD_DATE \
    VCS_REF

LABEL \
    "org.opencontainers.image.authors"="https://github.com/erigontech/erigon/graphs/contributors" \
    "org.opencontainers.image.base.name"="${TARGET_BASE_IMAGE}" \
    "org.opencontainers.image.created"="${BUILD_DATE}" \
    "org.opencontainers.image.revision"="${VCS_REF}" \
    "org.opencontainers.image.description"="Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency frontier." \
    "org.opencontainers.image.documentation"="https://docs.erigon.tech/" \
    "org.opencontainers.image.source"="https://github.com/erigontech/erigon" \
    "org.opencontainers.image.url"="https://github.com/erigontech/erigon/blob/main/Dockerfile"

STOPSIGNAL 2

SHELL ["/bin/bash", "-c"]

RUN --mount=type=bind,from=builder,source=/build,target=/tmp/build \
    echo Installing on ${TARGETARCH} with variant ${TARGETVARIANT} && \
    groupadd --gid ${GID_ERIGON} ${GROUP} && \
    useradd --system --uid ${UID_ERIGON} -g ${GROUP} --home /home/${USER} --shell /bin/bash ${USER} && \
    apt update -y && \
    apt install -y --no-install-recommends ca-certificates && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    install -d -o ${USER} -g ${GROUP} /home/${USER}/.local /home/${USER}/.local/share /home/${USER}/.local/share/erigon && \
    echo "Installing all binaries:" && \
    shopt -s extglob && \
    for binary in '/tmp/build/!(*.so)'; do \
        install -v -o root -g root $binary /usr/local/bin/ ; \
    done

VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}

USER ${USER}

EXPOSE ${EXPOSED_PORTS}

ENTRYPOINT [ "/usr/local/bin/erigon" ]
### End of Erigon Target section
