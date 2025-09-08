# `web3` RPC Namespace

## Introduction

The `web3` namespace provides utility methods that are part of the standard Ethereum JSON-RPC API. These methods offer basic functionality for client identification and cryptographic operations. In Erigon, the web3 namespace is implemented through the `Web3API` interface and `Web3APIImpl` struct.
The web3 namespace is enabled by default in Erigon's RPC daemon and provides essential utility functions that many Ethereum applications rely on for basic operations.

### Implementation Details
- The web3 namespace is implemented in `Web3APIImpl` which extends `BaseAPI` and uses the `ethBackend` for client version information
- The `web3_sha3` method uses Erigon's crypto library implementation of Keccak-256, which is the same hashing algorithm used throughout Ethereum
- Both methods are lightweight utility functions that don't require complex blockchain state access

### Usage Considerations
- `web3_clientVersion` is often used by applications to identify the Ethereum client type and version for compatibility checks
- `web3_sha3` provides the same Keccak-256 hashing that's used for Ethereum addresses, transaction hashes, and other cryptographic operations in the protocol
- These methods are part of the core Ethereum JSON-RPC specification and are supported by all major Ethereum clients

### Availability
- The web3 namespace is enabled by default in Erigon's RPC daemon
- No special configuration is required to use these methods
- They are available on both HTTP and WebSocket connections

---

## **web3_clientVersion**

Returns the current client version string, including the node name and version information.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type | Description |
| :---- | :---- |
| STRING | The current client version string including node name and version |

<cite>rpc/jsonrpc/web3_api.go:47-50</cite>

---

## **web3_sha3**

Returns Keccak-256 (not the standardized SHA3-256) of the given data. This method is commonly used for hashing arbitrary data using the same algorithm that Ethereum uses internally.

**Parameters**

| Parameter | Type | Description |
| :---- | :---- | :---- |
| data | DATA | The data to convert into a SHA3 hash |

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"web3_sha3","params":["0x68656c6c6f20776f726c64"],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type | Description |
| :---- | :---- |
| DATA | The SHA3 result of the given input string |
