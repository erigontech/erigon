---
description: Essential Utility Methods
---

# web3

The `web3` namespace provides utility methods that are part of the standard Ethereum JSON-RPC API. These methods offer basic functionality for client identification and cryptographic operations. In Erigon, the web3 namespace is implemented through the `Web3API` interface and `Web3APIImpl` struct. The web3 namespace is enabled by default in Erigon's RPC daemon and provides essential utility functions that many Ethereum applications rely on for basic operations.

### Implementation Details

* The web3 namespace is implemented in `Web3APIImpl` which extends `BaseAPI` and uses the `ethBackend` for client version information
* The `web3_sha3` method uses Erigon's crypto library implementation of Keccak-256, which is the same hashing algorithm used throughout Ethereum
* Both methods are lightweight utility functions that don't require complex blockchain state access

### Usage Considerations

* `web3_clientVersion` is often used by applications to identify the Ethereum client type and version for compatibility checks
* `web3_sha3` provides the same Keccak-256 hashing that's used for Ethereum addresses, transaction hashes, and other cryptographic operations in the protocol
* These methods are part of the core Ethereum JSON-RPC specification and are supported by all major Ethereum clients

### Availability

* The web3 namespace is enabled by default in Erigon's RPC daemon
* No special configuration is required to use these methods
* They are available on both HTTP and WebSocket connections

{% include "../../../.gitbook/includes/api-documentation-2.md" %}
