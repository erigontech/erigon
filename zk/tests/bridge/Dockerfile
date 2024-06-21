FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y curl jq git && \
    rm -rf /var/lib/apt/lists/*

RUN curl -L https://foundry.paradigm.xyz | bash && \
    /root/.foundry/bin/foundryup

ENV PATH="/root/.foundry/bin:$PATH"

COPY bridge.sh /usr/local/bin/bridge.sh
RUN chmod +x /usr/local/bin/bridge.sh

ENV RAW_PRIVATE_KEY bc6a95c870cce28fe9686fdd70a3a595769e019bb396cd725bc16ec553f07c83
ENV DESTINATION_NET 1
ENV DESTINATION_ADDRESS 0x0bb7AA0b4FdC2D2862c088424260e99ed6299148
ENV ETHER_VALUE 0.0200000054
ENV TOKEN_ADDRESS 0x0000000000000000000000000000000000000000
ENV IS_FORCED true
ENV BRIDGE_ADDRESS 0x528e26b25a34a4A5d0dbDa1d57D318153d2ED582
ENV META_BYTES 0x
ENV ETH_RPC_URL https://rpc.cardona.zkevm-rpc.com
ENV BRIDGE_API_URL https://bridge-api-cdk-validium-cardona-03-zkevm.polygondev.tools
ENV DRY_RUN true

CMD ["bash", "/usr/local/bin/bridge.sh"]