######
##  End-User usage note:
##
##  to build own docker image my-erigon-image:tag with just two binaries "erigon" and "downloader"
##  and with db_tools inside -- run following docker command in erigon/ directory:
##   
##    docker build --target erigon --build-arg BINARIES="erigon downloader" --build-arg BUILD_DBTOOLS="true"  --progress plain -t my-erigon-image:tag .
##
##  or simple build with default arguments (erigon only binary and without db-tools):
##
##    docker build --target erigon -t my-erigon-image:tag .
##
##  Note: build ARG "RELEASE_DOCKER_BASE_IMAGE" purposely defined incorrectly in order to fail "docker build"
######

## Note TARGETARCH is a crucial variable:
##   see https://docs.docker.com/reference/dockerfile/#automatic-platform-args-in-the-global-scope


ARG RELEASE_DOCKER_BASE_IMAGE="debian:12-slim" \
    CI_CD_MAIN_BUILDER_IMAGE="golang:1.24-bookworm" \
    CI_CD_MAIN_TARGET_BASE_IMAGE="debian:12-slim" \
    BUILDER_IMAGE="golang" \
    BUILDER_TAG="1.24-bookworm" \
    TARGET_IMAGE="debian" \
    TARGET_TAG="12-slim" \
    TARGETARCH \
    TARGETVARIANT \
    BINARIES="erigon" \
    BUILD_DBTOOLS="false" \
    BUILD_DATE="Not defined" \
    VCS_REF \
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
FROM docker.io/library/${BUILDER_IMAGE}:${BUILDER_TAG} AS builder 
ARG TARGETARCH \
    TARGETVARIANT \
    BUILD_DBTOOLS \
    BUILD_SILKWORM \
    BINARIES
WORKDIR /erigon

COPY . /erigon
SHELL ["/bin/bash", "-c"]
RUN --mount=type=cache,target=/go/pkg/mod \
    if [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "x" ]; then \
        echo "DEBUG: detected architecture AMD64v1"; \
        export AMD_FLAGS="GOAMD64_VERSION=v1 GOARCH=amd64"; \
    elif [ "x${TARGETARCH}" == "xamd64" ] && [ "x${TARGETVARIANT}" == "xv2" ]; then \
        echo "DEBUG: detected architecture AMD64v2"; \
        export AMD_FLAGS="GOAMD64_VERSION=v2 GOARCH=amd64"; \
    elif [ "x${BUILD_SILKWORM}" != "xtrue" ] || [ "x${TARGETARCH}" == "xarm64" ] ; then \
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
FROM docker.io/library/${TARGET_IMAGE}:${TARGET_TAG} AS erigon
ARG USER=erigon \
    GROUP=erigon \
    UID_ERIGON \
    GID_ERIGON \
    APPLICATION \
    BUILD_SILKWORM \
    TARGETARCH \
    TARGET_IMAGE \
    TARGET_TAG \
    EXPOSED_PORTS \
    BUILD_DATE \
    VCS_REF \
    BINARIES

LABEL \
    "org.opencontainers.image.authors"="https://github.com/erigontech/erigon/graphs/contributors" \
    "org.opencontainers.image.base.name"="${TARGET_IMAGE}:${TARGET_TAG}" \
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
    install -v -o root -g root /tmp/build/* /usr/local/bin/ && \
    rm -fv /usr/local/bin/libsilkworm_capi.so

VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}

USER ${USER}

EXPOSE ${EXPOSED_PORTS}

ENTRYPOINT [ "/usr/local/bin/erigon" ]
### End of Erigon Target section


### CI-CD : main branch docker image publishing for each new commit id
FROM ${CI_CD_MAIN_BUILDER_IMAGE} AS ci-cd-main-branch-builder

COPY /build-amd64 /build-amd64/
COPY /build-arm64 /build-arm64/

RUN echo "DEBUG: content of build-amd64" && ls -l /build-amd64 && \
    echo && \
    echo "DEBUG: content of build-arm64" && ls -l /build-arm64


FROM ${CI_CD_MAIN_TARGET_BASE_IMAGE} AS ci-cd-main-branch
ARG USER=erigon \
    GROUP=erigon \
    UID_ERIGON \
    GID_ERIGON \
    TARGETARCH \
    EXPOSED_PORTS

RUN --mount=type=bind,from=ci-cd-main-branch-builder,source=/build-${TARGETARCH},target=/tmp/erigon \
    addgroup --gid ${GID_ERIGON} ${GROUP} && \
    adduser --system --uid ${UID_ERIGON} --ingroup ${GROUP} --home /home/${USER} --shell /bin/bash ${USER} && \
    apt update -y && \
    apt install -y --no-install-recommends ca-certificates && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    install -d -o ${USER} -g ${GROUP} /home/${USER}/.local /home/${USER}/.local/share /home/${USER}/.local/share/erigon && \
    echo "Installing all binaries:" && \
    install -v -o root -g root /tmp/erigon/* /usr/local/bin/

VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}

USER ${USER}
EXPOSE ${EXPOSED_PORTS}

ENTRYPOINT [ "/usr/local/bin/erigon" ]

### End of CI-CD : main branch docker image publishing for each new commit id



### Release Dockerfile
FROM ${RELEASE_DOCKER_BASE_IMAGE} AS release-builder
ARG TARGETARCH \
    TARGETVARIANT \
    VERSION=${VERSION} \
    APPLICATION

COPY ${APPLICATION}_${VERSION}_linux_${TARGETARCH}${TARGETVARIANT}.tar.gz /tmp/${APPLICATION}.tar.gz

RUN tar xzvf /tmp/${APPLICATION}.tar.gz -C /tmp && \
    mv /tmp/${APPLICATION}_${VERSION}_linux_${TARGETARCH}${TARGETVARIANT} /tmp/${APPLICATION}

FROM ${RELEASE_DOCKER_BASE_IMAGE} AS release

ARG USER=erigon \
    GROUP=erigon \
    UID_ERIGON \
    GID_ERIGON \
    TARGETARCH \
    APPLICATION \
    EXPOSED_PORTS

STOPSIGNAL 2

SHELL ["/bin/bash", "-c"]

RUN --mount=type=bind,from=release-builder,source=/tmp/${APPLICATION},target=/tmp/${APPLICATION} \
    echo Installing on ${TARGETOS} with variant ${TARGETVARIANT} && \
    addgroup --gid ${GID_ERIGON} ${GROUP} && \
    adduser --system --uid ${UID_ERIGON} --ingroup ${GROUP} --home /home/${USER} --shell /bin/bash ${USER} && \
    apt update -y && \
    apt install -y --no-install-recommends ca-certificates && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    if [ "x${TARGETARCH}" == "xamd64" ]; then \
        echo "Installing libsilkworm_capi.so library to /lib/x86_64-linux-gnu/ in case amd64 architecture:"; \
        find /tmp/${APPLICATION} -name libsilkworm_capi.so -type f | xargs -I % install -m a=r -v % /lib/x86_64-linux-gnu/; \
        echo "Done." ; \
    fi && \
    install -d -o ${USER} -g ${GROUP} /home/${USER}/.local /home/${USER}/.local/share /home/${USER}/.local/share/erigon && \
    install -o root -g root /tmp/${APPLICATION}/erigon /usr/local/bin/ && \
    install -o root -g root /tmp/${APPLICATION}/integration /usr/local/bin/ && \
    install -o root -g root /tmp/${APPLICATION}/diag /usr/local/bin/ && \
    install -o root -g root /tmp/${APPLICATION}/sentry /usr/local/bin/ && \
    install -o root -g root /tmp/${APPLICATION}/txpool /usr/local/bin/ && \
    install -o root -g root /tmp/${APPLICATION}/downloader /usr/local/bin/ && \
    install -o root -g root /tmp/${APPLICATION}/rpcdaemon /usr/local/bin/

VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}

USER ${USER}

EXPOSE ${EXPOSED_PORTS}

ENTRYPOINT [ "/usr/local/bin/erigon" ]

### End of Release Dockerfile
