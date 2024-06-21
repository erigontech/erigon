FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sshpass \
    openssh-client \
    jq \
    curl \
    make \
    git \
    build-essential \
    bc \
    protobuf-compiler

ARG TARGETARCH
RUN case "${TARGETARCH}" in \
    "amd64") GO_TARBALL="go1.20.6.linux-amd64.tar.gz" ;; \
    "arm64") GO_TARBALL="go1.20.6.linux-arm64.tar.gz" ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -LO https://golang.org/dl/${GO_TARBALL} && \
    tar -C /usr/local -xzf ${GO_TARBALL} && \
    rm ${GO_TARBALL}

ENV PATH="/usr/local/go/bin:$PATH"

RUN pip install --no-cache-dir ansible

RUN git clone https://github.com/maticnetwork/polygon-cli.git /opt/polygon-cli && \
    cd /opt/polygon-cli && \
    make install && \
    mv $HOME/go/bin/polycli /usr/local/bin/polycli

RUN curl -L https://foundry.paradigm.xyz | bash -s -- --bin && \
    /root/.foundry/bin/foundryup

ENV PATH="/root/.foundry/bin:$PATH"

WORKDIR /ansible

COPY . /ansible

COPY run_playbooks.sh /usr/local/bin/run_playbooks.sh
RUN chmod +x /usr/local/bin/run_playbooks.sh

ENTRYPOINT ["run_playbooks.sh"]
