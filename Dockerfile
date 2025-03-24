## Custom Docker image could be built using following commands:
##
##   1. docker build -t ${my-local-image-name}:${my-tag} .
## 
##   2. docker build --build-arg BINARIES="erigon downloader evm" \
##        --build-arg BUILD_DBTOOLS="true" \
##        --progress plain \
##        -t ${my-local-image-name}:${my-tag} .
##
##   3. make docker
##
##   4. make docker DOCKER_BINARIES='erigon downloader evm'

ARG BUILDER_IMAGE="golang:1.24-bookworm" \
    TARGET_BASE_IMAGE="debian:12-slim" \
    BINARIES="erigon" \
    BUILD_DBTOOLS="false" \
    BUILD_DATE="Not defined" \
    VCS_REF="Not defined" \
    UID_ERIGON=1000 \
    GID_ERIGON=1000 \
    BUILD_SILKWORM="false" \
    VERSION=${VERSION} \
    APPLICATION="erigon" \
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

### Erigon Builder section:
FROM --platform=$BUILDPLATFORM ${BUILDER_IMAGE} AS builder 
ARG TARGETARCH \
    TARGETVARIANT \
    BUILD_DBTOOLS \
    BUILD_SILKWORM \
    BINARIES

WORKDIR /erigon

COPY . /erigon

SHELL ["/bin/bash", "-c"]

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache \
    echo "DEBUG: building on ${TARGETARCH}${TARGETVARIANT}" && \
    if [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "x" ]; then \
        echo "DEBUG: detected architecture AMD64v1"; \
        export AMD_FLAGS="GOAMD64_VERSION=v1 GOARCH=amd64"; \
    elif [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "xv2" ]; then \
        echo "DEBUG: detected architecture AMD64v2"; \
        export AMD_FLAGS="GOAMD64_VERSION=v2 GOARCH=amd64"; \
    elif [ "x${TARGETARCH}" == "xarm64" ]; then \
        echo "DEBUG: detected architecture ARM64"; \
        export AMD_FLAGS="GOARCH=arm64"; \
    fi && \
    if [ "x${BUILD_SILKWORM}" != "xtrue" ] || [ "x${TARGETARCH}" == "xarm64" ] ; then \
        echo "DEBUG: add nosilkworm build tag - BUILD_SILKWORM is not true OR ARM64 architecture "; \
        export FLAG_SILKWORM=",nosilkworm"; \
    fi && \
    echo "DEBUG: cmd - make ${AMD_FLAGS} ${BINARIES} GOBIN=/build FLAG_SILKWORM=${FLAG_SILKWORM} ." && \
    make ${AMD_FLAGS} ${BINARIES} GOBIN=/build BUILD_TAGS=nosqlite,noboltdb${FLAG_SILKWORM} && \
    if [ "x${BUILD_SILKWORM}" == "xtrue" ] && [ "x${TARGETARCH}" == "xamd64" ]; then \
        echo "DEBUG: BUILD_SILKWORM=${BUILD_SILKWORM} - installing libsilkworm_capi.so lib on architecture ARM64"; \
        find $(go env GOMODCACHE)/github.com/erigontech -name libsilkworm_capi.so -exec install {} /build \; ;\
    fi && \
    if [ "x${BUILD_DBTOOLS}" == "xtrue" ]; then \
        echo "Building db-tools:"; \
        make GOBIN=/build db-tools; \
    fi && \
    find /build -ls

### End of builder section


### Erigon Target section:
FROM ${TARGET_BASE_IMAGE} AS erigon
ARG USER=erigon \
    GROUP=erigon \
    UID_ERIGON \
    GID_ERIGON \
    APPLICATION \
    BUILD_SILKWORM \
    TARGETARCH \
    TARGET_BASE_IMAGE \
    EXPOSED_PORTS \
    BUILD_DATE \
    VCS_REF \
    BINARIES

LABEL \
    "org.opencontainers.image.authors"="https://github.com/erigontech/erigon/graphs/contributors" \
    "org.opencontainers.image.base.name"="${TARGET_BASE_IMAGE}" \
    "org.opencontainers.image.created"="${BUILD_DATE}" \
    "org.opencontainers.image.revision"="${VCS_REF}" \
    "org.opencontainers.image.description"="Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency frontier." \
    "org.opencontainers.image.documentation"="https://erigon.gitbook.io/erigon" \
    "org.opencontainers.image.source"="https://github.com/erigontech/erigon/blob/main/Dockerfile" \
    "org.opencontainers.image.url"="https://github.com/erigontech/erigon/blob/main/Dockerfile"

STOPSIGNAL 2

SHELL ["/bin/bash", "-c"]

RUN --mount=type=bind,from=builder,source=/build,target=/tmp/build \
    echo Installing on ${TARGETARCH} with variant ${TARGETVARIANT} && \
    addgroup --gid ${GID_ERIGON} ${GROUP} && \
    adduser --system --uid ${UID_ERIGON} --ingroup ${GROUP} --home /home/${USER} --shell /bin/bash ${USER} && \
    apt update -y && \
    apt install -y --no-install-recommends ca-certificates && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    if [ "x${TARGETARCH}" == "xamd64" ] && [ "x${BUILD_SILKWORM}" != "xtrue" ]; then \
        echo "Installing libsilkworm_capi.so library to /lib/x86_64-linux-gnu/ in case amd64 architecture:"; \
        find /tmp/build -name libsilkworm_capi.so -type f | xargs -I % install -m a=r -v % /lib/x86_64-linux-gnu/; \
        echo "Done." ; \
    fi && \    
    install -d -o ${USER} -g ${GROUP} /home/${USER}/.local /home/${USER}/.local/share /home/${USER}/.local/share/erigon && \
    echo "Installing all binaries:" && \
    for binaries in ${BINARIES}; do \
        install -v -o root -g root /tmp/build/$binaries /usr/local/bin/ ; \
    done

VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}

USER ${USER}

EXPOSE ${EXPOSED_PORTS}

ENTRYPOINT [ "/usr/local/bin/erigon" ]
### End of Erigon Target section