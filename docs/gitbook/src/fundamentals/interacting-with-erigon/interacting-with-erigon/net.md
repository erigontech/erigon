---
description: Network Connectivity and Peer Information
---

# net

The `net` namespace provides network-related methods that are part of the standard Ethereum JSON-RPC API. These methods offer information about the node's network connectivity, peer count, and network version. In Erigon, the net namespace is implemented through the `NetAPI` interface and `NetAPIImpl` struct.

The `net` namespace is enabled by default in Erigon's RPC daemon and provides essential network information that applications use to understand the node's connectivity status and network configuration.

### Implementation Details

* The net namespace is implemented in `NetAPIImpl` which uses the `ethBackend` to access network information.
* `net_listening` determines connectivity by attempting to retrieve peer information from the backend.
* `net_version` and `net_peerCount` require access to the backend and will return errors in `--datadir` mode when the backend is unavailable.

### Backend Dependency

* Methods `net_version` and `net_peerCount` require the `ethBackend` to be available.
* When running in `--datadir` mode or when the backend cannot be accessed, these methods will return a "not available" error.
* The `net_listening` method gracefully handles backend unavailability by returning `false` .

### Network Information Usage

* `net_version` is commonly used by applications to verify they're connected to the correct .Ethereum network.
* `net_peerCount` helps monitor node connectivity and network health.
* `net_listening` provides a basic connectivity check for the node's network interface.

### Availability and Configuration

* The net namespace is enabled by default in Erigon's RPC daemon.
* These methods are available on both HTTP and WebSocket connections.
* For remote RPC daemon setups, the `net` namespace must be explicitly enabled for health check functionality.

### Documentation References

* The current implementation shows some limitations noted in the documentation, such as hardcoded return values in certain scenarios.
* `net_peerCount` specifically counts only internal sentries, which may not reflect the total peer count in distributed setups.

{% include "../../../.gitbook/includes/api-documentation-2.md" %}
