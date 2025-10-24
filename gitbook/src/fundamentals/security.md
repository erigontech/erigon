---
description: >-
  Securing Your Erigon Node: Best Practices for RPC, Network, and Operational
  Safety
---

# Security

The security practices focus heavily on the RPC daemon since it's the primary external interface. The modular architecture allows you to run components separately, which can improve security by isolating services. For production deployments, consider using the distributed mode where services communicate via secured gRPC rather than running everything in a single process.

## Network Security

**Firewall Configuration**: Secure your network ports properly. The default ports include:

* Port `30303` for P2P networking
* Port `8545` for JSON-RPC (if enabled)
* Port `9090` for private API communication

**CORS Protection**: When exposing RPC endpoints, avoid using wildcard CORS domains. Instead, set specific hostnames or IP addresses to prevent cross-origin attacks.

## RPC Security

**Method Allowlisting**: Restrict available RPC methods using access control lists. Create a `rules.json` file specifying only the methods you need:

```json
{
  "allow": ["net_version", "web3_eth_getBlockByHash"]
}
```

**Remove Admin APIs**: Don't include `admin` in your `--http.api` list for public-facing nodes.

**Rate Limiting**: Protect against DOS attacks by configuring `--rpc.batch.concurrency` and `--rpc.batch.limit`.

{% include "../.gitbook/includes/warning-admin_-and-debug_-....md" %}

## TLS and Authentication

**TLS Encryption**: For remote RPC daemon deployments, enable TLS authentication. The process involves:

1. Generating CA key pairs
2. Creating certificates for each instance
3. Deploying certificates to secure communication

**Private API Security**: Configure secure communication between RPC daemon and Erigon instance using TLS certificates.

## Operational Security

**Dedicated User**: Run Erigon as a dedicated system user rather than root to limit potential damage from security breaches.

**Transaction Pool Security**: Use `--txpool.nolocals=true` for public nodes to prevent local transaction injection.

**Virtual Host Protection**: Configure `HTTPVirtualHosts` to prevent DNS rebinding attacks. This validates the Host header to ensure requests come from authorized domains.

## Health Monitoring

**Health Checks**: Implement monitoring using the `/health` endpoint to detect issues early. Configure minimum peer counts and block validation to ensure your node stays healthy.
