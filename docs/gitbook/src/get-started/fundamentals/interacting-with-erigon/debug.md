---
description: 'Erigon RPC debug Namespace: Deep Diagnostics and State Introspection'
---

# debug

The `debug` namespace provides debugging and diagnostic methods for Erigon node operators and developers. These methods offer deep introspection into blockchain state, transaction execution, and node performance. The debug namespace is implemented through the `PrivateDebugAPI` interface and `DebugAPIImpl` struct.

The debug namespace must be explicitly enabled using the `--http.api` flag when starting the RPC daemon. For security reasons, these methods are considered private and should not be exposed on public RPC endpoints.

### Security and Access Control

* Debug methods are considered private and should not be exposed on public RPC endpoints;
* These methods can consume significant resources and should be used carefully in production environments;
* Access should be restricted to trusted operators and developers only.

### Performance Considerations

* Tracing methods (`debug_traceTransaction`, `debug_traceBlockByHash`, etc.) support streaming to handle large results efficiently;
* The `AccountRangeMaxResults` constant limits account range queries to 8192 results, or 256 when storage is included;
* Memory and GC control methods allow fine-tuning of node performance.

### Integration with Erigon Architecture

* Debug methods leverage Erigon's temporal database for historical state access;
* The implementation uses `kv.TemporalRoDB` for efficient historical queries;
* Tracing functionality integrates with Erigon's execution engine and EVM implementation.

### Usage in Development and Testing

* These methods are essential for debugging transaction execution issues;
* Storage range methods help analyze contract state changes;
* Memory management methods assist in performance optimization and resource monitoring.

{% include "../../../.gitbook/includes/api-documentation-3.md" %}
