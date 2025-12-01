# gRPC

Erigon provides gRPC APIs that allow users to access blockchain data and services directly through protocol buffer interfaces. These APIs offer high-performance, strongly-typed access to Erigon's internal services and are particularly useful for applications requiring efficient data access or integration with other gRPC-based systems.

The gRPC server must be explicitly enabled using the `--grpc` flag when starting the RPC daemon, and can be configured with custom listening addresses, ports, and TLS settings.

### Performance Considerations

* gRPC APIs provide better performance than JSON-RPC for high-throughput applications
* Direct database access via KV interface offers the fastest data retrieval
* Shared memory access (when running locally) provides optimal performance

### Data Format and Buckets

Database bucket names and their formats are documented in `db/kv/tables.go`. Understanding these structures is essential for effective use of the KV interface.

### Network Access

* gRPC services can be accessed over the network when properly configured
* TLS encryption is recommended for production deployments
* Rate limiting can be configured via `--private.api.ratelimit` flag

### Integration Libraries

Erigon provides Go, Rust, and C++ implementations of the RoKV (read-only key-value) interface for easy integration with applications.

### Availability

* gRPC services are available when enabled with the `--grpc` flag
* Default listening address is configurable via `--grpc.addr` and `--grpc.port`
* Services require the main Erigon node to be running and accessible

For more information, visit the [Erigon Interfaces GitHub repository](https://github.com/erigontech/interfaces).

***

## **KV (Key-Value) Interface**

The KV interface provides low-level database access methods for reading blockchain data directly from Erigon's MDBX database.

**Interface Definition**

The KV interface is defined in the remote protobuf specification and provides methods for:

* Opening read-only transactions
* Reading data by key ranges
* Iterating over database buckets
* Accessing historical state data

**Example Usage**

```bash
# Connect to gRPC endpoint
grpcurl -plaintext localhost:9090 remote.KV/Version
```

**Available Methods**

| Method       | Description                                   |
| ------------ | --------------------------------------------- |
| Version      | Returns the KV service API version            |
| Tx           | Opens a read-only transaction for data access |
| StateChanges | Streams state changes for a block range       |
| Snapshots    | Returns information about available snapshots |

***

## **ETHBACKEND Interface**

The ETHBACKEND interface provides access to Ethereum-specific backend services including block data, transaction information, and network status.

**Available Services**

| Service         | Description                                         |
| --------------- | --------------------------------------------------- |
| Etherbase       | Returns the coinbase address                        |
| NetVersion      | Returns the network ID                              |
| NetPeerCount    | Returns the number of connected peers               |
| ProtocolVersion | Returns the Ethereum protocol version               |
| ClientVersion   | Returns the client version string                   |
| Subscribe       | Subscribes to blockchain events                     |
| NodeInfo        | Returns node information                            |
| Peers           | Returns information about connected peers           |
| AddPeer         | Adds a peer to the node                             |
| PendingBlock    | Returns the pending block                           |
| BorEvent        | Returns Bor-specific events (Polygon networks only) |

***

## **TxPool Interface**

The TxPool interface provides access to transaction pool operations and status information.

**Available Methods**

| Method            | Description                               |
| ----------------- | ----------------------------------------- |
| Version           | Returns the TxPool service version        |
| FindUnknownHashes | Finds unknown transaction hashes          |
| GetTransactions   | Retrieves transactions from the pool      |
| All               | Returns all transactions in the pool      |
| PendingAdd        | Adds transactions to pending pool         |
| PendingRemove     | Removes transactions from pending pool    |
| OnAdd             | Subscribes to transaction addition events |
| Mining            | Returns mining-related transaction data   |
| NonceFromAddress  | Gets the next nonce for an address        |

***

## **Downloader Interface**

The Downloader interface provides access to snapshot downloading and torrent management functionality.

**Available Methods**

| Method       | Description                        |
| ------------ | ---------------------------------- |
| Add          | Adds files to download queue       |
| Delete       | Removes files from download queue  |
| Completed    | Checks if downloads are completed  |
| SetLogPrefix | Sets logging prefix for downloader |

***

## Polygon Bridge Backend gRPC API

These gRPC APIs are specifically designed for Polygon's Bor consensus mechanism and are only active when running Erigon with Polygon network configuration. The services provide essential functionality for bridge event processing and validator management required by the Polygon network architecture.

### Bridge Backend Methods

**Version Method:**

* `Version()` - Returns the service version number.

**Transaction Lookup:**

* `BorTxnLookup(BorTxnLookupRequest)` - Looks up Bor transaction information by hash.

**Event Retrieval:**

* `BorEvents(BorEventsRequest)` - Retrieves bridge events for a specific block.

### Bridge Backend Implementation

The server implementation is found in `polygon/bridge/server.go` where the `BackendServer` struct implements the `BridgeBackendServer` interface:

* The `BorTxnLookup` method implementation shows how it handles transaction lookups.
* The `BorEvents` method retrieves bridge events for a given block.

## Heimdall Backend gRPC API

The Heimdall Backend service provides APIs for validator and consensus-related functionality.

### Heimdall Backend Methods

**Version Method:**

* `Version()` - Returns the service version number.

**Producer Information:**

* `Producers(BorProducersRequest)` - Retrieves validator/producer information for a specific block.

## Service Integration

Both services are integrated into the main Ethereum backend when Bor consensus is configured. In the main backend initialization, you can see how these services are set up.

The Bridge and Heimdall services are created with their respective RPC servers.

***

## Configuration and Security

### TLS Configuration

Erigon supports TLS encryption for gRPC connections using certificate files:

{% code overflow="wrap" %}
```bash
./build/bin/rpcdaemon --grpc --tls.cert=/path/to/cert.pem --tls.key=/path/to/key.pem --tls.cacert=/path/to/ca.pem
```
{% endcode %}

### Health Checks

gRPC health checks can be enabled to monitor service availability:

```bash
./build/bin/rpcdaemon --grpc --grpc.healthcheck
```

### Connection Examples

**Go Client Example**

```go
conn, err := grpc.Dial("localhost:9090", grpc.WithInsecure())
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

client := remote.NewKVClient(conn)
// Use client for database operations
```

**Direct Database Access**

The gRPC interface allows reading Erigon's database while the node is running, sharing the same OS-level PageCache for optimal performance.
