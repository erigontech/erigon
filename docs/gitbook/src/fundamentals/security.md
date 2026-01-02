---
description: >-
  Securing Your Erigon Node: Best Practices for RPC, Network, and Operational
  Safety
---

# Security

The security practices focus heavily on the RPC daemon since it's the primary external interface. The modular architecture allows you to run components separately, which can improve security by isolating services. For production deployments, consider using the distributed mode where services communicate via secured gRPC rather than running everything in a single process.

## Network Security

Securing your network ports is essential for protecting your Erigon node. Proper firewall configuration is the first line of defense.&#x20;

Based on best practices for execution clients, your local machine's firewall settings should be configured as follows:

| **Port** | **Protocol** | **Purpose**                   | **Firewall Action**                                                                                                                      |
| -------- | ------------ | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| 30303    | TCP & UDP    | Peer-to-Peer (P2P) Networking | Allow inbound and outbound traffic to enable peer discovery and connections.                                                             |
| 8545     | TCP          | JSON-RPC (Public API)         | Block all traffic _except_ from explicitly defined trusted machines. This port should never be publicly exposed without strict controls. |
| 9090     | TCP          | Private API Communication     | Block all traffic except for communication between your internal components (e.g., RPC daemon and core node).                            |

#### CORS Protection

When exposing public RPC endpoints (like those on port 8545), use the following practice to mitigate cross-origin attacks:

* Avoid using a wildcard `*` for Cross-Origin Resource Sharing (CORS) domains.
* Set specific hostnames or IP addresses for CORS to ensure only authorized frontend applications can interact with your RPC service.

## API Security

The RPC daemon is the primary external interface, making API security critical. To protect against abuse, denial-of-service (DoS) attacks, OOM issues and unauthorized access, implement the following controls:

| **Security Measure** | **Description**                                                                                                                                                                                                   | **Erigon Configuration**                                                                                                                                                             |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Method Allowlisting  | <p>Restrict the available RPC methods to only those strictly necessary. This significantly reduces the attack surface. </p><p></p><p>Can be applied to <code>erigon</code> or <code>rpcdaemon</code> process.</p> | Use the `--rpc.accessList=rules.json` flag, pointing to a JSON file (e.g., `rules.json`) that specifies the allowed methods: `{"allow": ["net_version", "eth_getBlockByHash"]}`      |
| Remove Admin APIs    | Never include sensitive namespaces like `admin` or `debug` in the `--http.api` list for public-facing nodes. These APIs are intended for node operators only.                                                     | Omit the `admin` and `debug` namespaces from `--http.api`.                                                                                                                           |
| Rate Limiting        | Protect against DoS attacks by limiting the processing capacity for batch requests.                                                                                                                               | Configure `--rpc.batch.concurrency` and `--rpc.batch.limit` flags.                                                                                                                   |
| Subscription Filters | Control WebSocket subscriptions to prevent Out-of-Memory (OOM) errors caused by a large number of filter requests.                                                                                                | <p>Utilize the various <code>--rpc.subscription.filters.*</code> flags.</p><p><strong>Note</strong>: These are disabled by default because they increase the risk of OOM issues.</p> |

### External Protection Layer (Recommended for Production)

For production environments where RPC endpoints are exposed publicly, it is strongly recommended to place the Erigon node behind a robust proxy layer. This layer should be responsible for:

* **Application-Level Filtering**: Implementing more granular logic than basic allowlists.
* **Rate Limiting**: Providing centralized, high-performance rate limiting.
* **TLS Termination**: Handling SSL/TLS encryption/decryption.
* **Monitoring and Logging**: Tracking requests for security auditing.
* **Web Application Firewalls (WAFs)**: Protecting against common web exploits.

{% include "../../.gitbook/includes/warning-admin_-and-debug_-....md" %}

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
