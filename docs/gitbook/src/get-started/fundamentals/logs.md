---
description: Erigon's logging framework
---

# Logs

Erigon features a sophisticated logging framework that offers detailed visibility into the synchronization process and operational status. This system provides comprehensive, structured logs suitable for both human operators and automated monitoring, while maintaining high performance.

The modular, staged approach to logging allows for granular control over verbosity, which is crucial for precise debugging and flexible deployment across various environments.&#x20;

{% hint style="success" %}
Erigon offers a `--metrics` flag for using prometheus/grafana monitoring, see [Monitoring](../../archive/monitoring/).
{% endhint %}

## Logging Framework Architecture

### Core Logging System

Erigon implements a custom logging framework that supports structured logging with key-value pairs and multiple output handlers.

### Configuration Options

Erigon provides extensive logging configuration through command-line flags. Key configuration options include:

* `--log.json`: Enable JSON formatting for console logs
* `--verbosity`: Set console log level (default: `2`)
* `--log.dir.path`: Specify directory for log files. By default Erigon writing logs to `datadir/logs` directory.
* `--log.dir.verbosity`: Set file log level
* `--log.delays`: Enable block delay logging

**Log Levels**

The logging system defines six distinct log levels in hierarchical order:

* **LvlCrit (0)**: Critical errors that may cause application termination
* **LvlError (1)**: Error conditions that require attention
* **LvlWarn (2)**: Warning messages for potentially problematic situations
* **LvlInfo (3)**: General informational messages
* **LvlDebug (4)**: Detailed debugging information
* **LvlTrace (5)**: Most verbose tracing information

The log level is set by using the `--verbosity` flag, for example:

```
./build/bin/erigon --verbosity=1
```

**Logger Interface**

The core Logger interface provides methods for each log level and supports contextual logging.

## Staged Synchronization Architecture

### Stage Definitions

Erigon's synchronization process is organized into sequential stages, each handling specific aspects of blockchain data processing.

The primary synchronization stages include:

* **Snapshots (OtterSync)**: Download and process blockchain snapshots
* **Headers**: Download and validate block headers
* **BlockHashes**: Generate block number to hash mappings
* **Bodies**: Download and validate block bodies
* **Senders**: Recover transaction senders from signatures
* **Execution**: Execute transactions and update state
* **TxLookup**: Generates transaction lookup indices. This indexing is essential for quickly finding transaction by its hash, significantly improving the performance of transaction-related RPC calls.
* **Finish**: The finalization stage of the sync process. This is the point where the node sends out notifications to subscribers about the new blockchain head, ensuring other components and external applications are instantly aware of the latest block.

### Stage Progress Tracking

Each stage maintains progress information in the database, allowing for resumable synchronization.

## Log Message Structure

### Structured Logging Format

All log messages follow a consistent structured format with key-value pairs for machine parsing and human readability.

**Standard Fields:**

* Timestamp (t)
* Log level (lvl)
* Message (msg)
* Contextual key-value pairs

### Prefix System

The sync engine uses a sophisticated prefix system to identify stage context.

The prefix format includes stage position and total count (e.g., "1/10 Headers") for easy identification of sync progress.
