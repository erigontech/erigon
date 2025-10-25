# net

The `net` namespace provides network-related methods that are part of the standard Ethereum JSON-RPC API. These methods offer information about the node's network connectivity, peer count, and network version. In Erigon, the net namespace is implemented through the `NetAPI` interface and `NetAPIImpl` struct.

The `net` namespace is enabled by default in Erigon's RPC daemon and provides essential network information that applications use to understand the node's connectivity status and network configuration.

### Implementation Details

* The net namespace is implemented in `NetAPIImpl` which uses the `ethBackend` to access network information
* `net_listening` determines connectivity by attempting to retrieve peer information from the backend
* `net_version` and `net_peerCount` require access to the backend and will return errors in `--datadir` mode when the backend is unavailable

### Backend Dependency

* Methods `net_version` and `net_peerCount` require the `ethBackend` to be available
* When running in `--datadir` mode or when the backend cannot be accessed, these methods will return a "not available" error
* The `net_listening` method gracefully handles backend unavailability by returning `false`

### Network Information Usage

* `net_version` is commonly used by applications to verify they're connected to the correct Ethereum network
* `net_peerCount` helps monitor node connectivity and network health
* `net_listening` provides a basic connectivity check for the node's network interface

### Availability and Configuration

* The net namespace is enabled by default in Erigon's RPC daemon
* These methods are available on both HTTP and WebSocket connections
* For remote RPC daemon setups, the `net` namespace must be explicitly enabled for health check functionality

### Documentation References

* The current implementation shows some limitations noted in the documentation, such as hardcoded return values in certain scenarios
* `net_peerCount` specifically counts only internal sentries, which may not reflect the total peer count in distributed setups

***

## **net\_listening**

Returns `true` if the client is actively listening for network connections. The method checks if the node can retrieve peer information, indicating that the network interface is up and listening.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"net_listening","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type    | Description                              |
| ------- | ---------------------------------------- |
| Boolean | `true` when listening, `false` otherwise |

***

## **net\_version**

Returns the current network ID as a string. This identifies which Ethereum network the node is connected to (e.g., "1" for Mainnet, "11155111" for Sepolia).

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"net_version","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type   | Description                                                                             |
| ------ | --------------------------------------------------------------------------------------- |
| STRING | The current network ID (e.g., "1" for Ethereum Mainnet, "11155111" for Sepolia Testnet) |

***

## **net\_peerCount**

Returns the number of peers currently connected to the first sentry server. This provides insight into the node's network connectivity and peer relationships.

**Parameters**

None

**Example**

```bash
curl -s --data '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":"1"}' -H "Content-Type: application/json" -X POST http://localhost:8545
```

**Returns**

| Type     | Description                              |
| -------- | ---------------------------------------- |
| QUANTITY | Number of connected peers as hexadecimal |
