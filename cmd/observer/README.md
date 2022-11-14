# Observer - P2P network crawler

Observer crawls the Ethereum network and collects information about the nodes.

### Build

    make observer

### Run

    observer --datadir ... --nat extip:<IP> --port <PORT>

Where `IP` is your public IP, and `PORT` has to be open for incoming UDP traffic.

See `observer --help` for available options.

### Report

To get the report about the currently known network state run:

    observer report --datadir ...

## Description

Observer uses [discv4](https://github.com/ethereum/devp2p/blob/master/discv4.md) protocol to discover new nodes.
Starting from a list of preconfigured "bootnodes" it uses FindNode
to obtain their "neighbor" nodes, and then recursively crawls neighbors of neighbors and so on.
Each found node is re-crawled again a few times.
If the node fails to be pinged after maximum attempts, it is considered "dead", but still re-crawled less often.

A separate "diplomacy" process is doing "handshakes" to obtain information about the discovered nodes.
It tries to get [RLPx Hello](https://github.com/ethereum/devp2p/blob/master/rlpx.md#hello-0x00)
and [Eth Status](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#status-0x00)
from each node.
The handshake repeats a few times according to the configured delays.
