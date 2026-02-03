---
description: >-
  Unlocking Erigon's Connectivity: How --nat Shapes Your Node's Network Reach
  and Performance
---

# NAT

### What `--nat` really controls

The `--nat` option controls how Erigon advertises its external address to other peers in the Ethereum P2P network.

It does not:

* open firewall ports for you
* guarantee inbound connectivity by itself
* directly control peer count

Instead, it determines whether other nodes can initiate inbound connections to your node, which has a significant impact on:

* peer diversity (inbound vs outbound)
* transaction gossip freshness
* block content quality for validators and block producers

### Why NAT configuration matters more than peer count

Ethereum P2P is bidirectional:

* your node connects outbound to peers
* other nodes may connect inbound to you

Nodes that are reachable from the internet (i.e. correctly advertised via NAT) tend to:

* receive transactions earlier
* receive a wider variety of transactions
* maintain a healthier “pending” transaction pool

For validators and block producers, this directly affects:

* transaction inclusion
* gas usage
* likelihood of producing empty or low-gas blocks

A high peer count alone does not guarantee good transaction propagation if most peers are outbound-only.

## NAT modes explained (practical behavior)

### `nat: none`

Disables external address advertisement.

Erigon will not advertise a reachable address to peers. In practice, this often results in:

* mostly outbound connections
* few or no inbound peers
* delayed or stale transaction gossip

**Important**: `nat: none` is generally not recommended for validators or block producers, even if peer count appears high. It is primarily suitable for:

* private networks
* non-proposing archive / RPC nodes
* restricted environments where inbound connectivity is intentionally disabled

### `nat: extip:` (recommended for datacenters & VPS)

Explicitly advertises the given public IPv4 address to peers.

This is the most reliable and deterministic option when:

* your node has a stable public IPv4 address
* required P2P ports are open in the firewall

Benefits:

* enables inbound peer connections
* improves transaction gossip freshness
* leads to healthier txpool “pending” state
* strongly recommended for validators

Example:

```
nat: "extip:203.0.113.114"
```

### `nat: any`

Enables automatic NAT detection (e.g. UPnP / NAT-PMP where available).

Suitable for:

* home networks
* environments where the external IP is not known in advance

Less deterministic than extip, but generally better than none.

### `nat: stun`

Uses STUN to discover the external address.

Useful when:

* running behind NAT
* no UPnP is available
* external IP cannot be configured manually
