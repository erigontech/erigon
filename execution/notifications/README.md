# Notification Flows

This package defines the types and interfaces for Erigon's execution notification
system. Notifications carry state changes, headers, logs, and receipts from the
execution pipeline to downstream consumers (TxPool, RPC daemon, P2P).

## Architecture

State changes use protobuf messages for both in-process and remote consumers.
The dispatcher prepares the batch in native Go types and converts it before sending.
Logs and receipts use native types in-process and are converted at the gRPC boundary.

## Flow 1: State Changes

State changes track account/storage/code modifications per block. Accumulated during
block execution, dispatched after the pipeline run.

### Sender

```
exec3 block execution (execution/stagedsync/exec3_serial.go, exec3_parallel.go)
  → Accumulator.StartChange(header, txs, unwind)     per block
  → Accumulator.ChangeAccount(addr, incarnation, data)
  → Accumulator.ChangeStorage(addr, incarnation, loc, data)
  → Accumulator.ChangeCode(addr, incarnation, code)
  → Accumulator.DeleteAccount(addr)

Dispatcher.Dispatch()  (execution/execmodule/notification_dispatcher.go)
  → prepareStateChanges()
    → Accumulator.Batch(executedHeader)
    → set state version, pending fees, gas limit and finalized height
    → BlockBatchNotification.RefreshAccounts(state)
  → publish headers, receipts and logs
  → StateChangeConsumer.SendStateChanges(batch.ToProtoBatch())
  → Accumulator.Reset()
```

Normal dispatch uses the header at Finish stage progress so the announced head
matches the supplied state. Stored head markers can be ahead of execution.

Preparation keeps recorded execution changes intact. Synthetic head entries and
final account values are added only to the output batch. A preparation failure
leaves the accumulator available for retry and publishes no events.

### Prepared Batch

```
BlockBatchNotification contains:
  - StateVersionID      uint64
  - StateChangesIncomplete bool
  - Changes             []StateChange  (native types)
    - Direction          (Forward/Unwind)
    - BlockHeight        uint64
    - Txs                [][]byte
    - Changes            []AccountChange
  - PendingBlockBaseFee  uint64
  - PendingBlobFee       uint64
  - BlockGasLimit        uint64
  - FinalizedBlock       uint64
```

### Delivery

```
StateChangeConsumer.SendStateChanges(StateChangeBatch)  [protobuf]
  → KvServer (db/kv/remotedbserver/)
    → StateChangePubSub.Pub()
      → StateChanges stream
        → TxPool Fetch.handleStateChanges()
        → rpcdaemon state cache
```

TxPool uses transaction directions to track mined and unwound transactions, and
account/storage/code changes to refresh sender state and invalidate frame validations.

On subscription, the KV server reads a reset at committed Execution stage progress.
Under the publication lock, it selects the newer of this reset and the latest published
head, keeping the head and state version together. It sends that reset before later
batches, including when the latest publication is still awaiting commit.

For incomplete batches, `RefreshAccounts` appends final values for addresses with
recorded account changes. This keeps older entries from overwriting current values
in the RPC state cache, but it does not reconstruct omitted changes.

When the batch is incomplete or the pool's state view is stale, the pool waits for
committed state matching the announced version before refreshing all pooled senders
and revalidating frames. If that version has been superseded, transaction membership
changes are retained for a newer batch; obsolete account values are not applied.
Complete batches can be applied before commit while the pool has a usable state view.

### Consumer Summary

| Consumer | Reads |
|----------|-------|
| TxPool | Head, state version, transactions, fees, account/storage/code changes, completeness |
| kvcache.Coherent | State version, account/storage/code changes, completeness |

---

## Flow 2: Headers

New block headers are sent to RPC subscribers (`eth_subscribe("newHeads")`).

### Sender

```
Dispatcher.Dispatch()
  → stagedsync.NotifyNewHeaders(from, to, events, tx)
    reads raw header RLP from DB or SD overlay
    → Events.OnNewHeader(headersRlp [][]byte)
```

### Local Path

```
Events.headerSubscriptions  (chan [][]byte)
  → EthBackendServer (node/privateapi/ethbackend.go)
    wraps RLP in SubscribeReply{Type: Event_HEADER}
```

### Remote Path

```
EthBackendServer.Subscribe()  gRPC stream
  → RPC Filters (rpc/rpchelper/filters.go)
    → rlp.DecodeBytes → types.Header
    → eth_subscribe("newHeads") websocket → JSON
```

Header RLP is the canonical format — minimal overhead.

---

## Flow 3: Logs

Log events from executed transactions, sent to `eth_subscribe("logs")` subscribers.

### Sender

```
exec3 block execution
  → RecentReceipts.Add(receipts, txs, header)   stores native types

Dispatcher.Dispatch()
  → RecentReceipts.NotifyLogs(events, from, to)
    → Events.OnLogs([]*LogNotification)          native types
```

### Local Path

```
Events.logsSubscriptions  (chan []*LogNotification)
  → node/privateapi/LogsFilterAggregator
    filters by address/topics
```

### Remote Path (gRPC boundary conversion)

```
node/privateapi/ethbackend.go
  converts LogNotification → remoteproto.SubscribeLogsReply
  → gRPC stream to RPC daemon
    → rpc/rpchelper/LogsFilterAggregator
      → eth_subscribe("logs") websocket → types.Log → JSON
```

### Consumer Summary

| Consumer | Mode | Receives |
|----------|------|----------|
| LogsFilterAggregator (privateapi) | local | `*LogNotification` (native) |
| RPC daemon | remote | `SubscribeLogsReply` (protobuf via gRPC) |
| eth_subscribe("logs") | websocket | `types.Log` → JSON |

---

## Flow 4: Receipts

Transaction receipts, sent to `eth_subscribe` receipt subscribers.

### Sender

```
exec3 block execution
  → RecentReceipts.Add(receipts, txs, header)   stores native types

Dispatcher.Dispatch()
  → RecentReceipts.NotifyReceipts(events, from, to)
    → Events.OnReceipts([]*ReceiptNotification)  native types
```

### Local Path

```
Events.receiptsSubscriptions  (chan []*ReceiptNotification)
  → node/privateapi/ReceiptsFilterAggregator
    filters by transaction hash
```

### Remote Path (gRPC boundary conversion)

```
node/privateapi/ethbackend.go
  converts ReceiptNotification → remoteproto.SubscribeReceiptsReply
  → gRPC stream to RPC daemon
    → rpc/rpchelper/ReceiptsFilterAggregator
      → MarshalSubscribeReceipt → JSON
```

### Consumer Summary

| Consumer | Mode | Receives |
|----------|------|----------|
| ReceiptsFilterAggregator (privateapi) | local | `*ReceiptNotification` (native) |
| RPC daemon | remote | `SubscribeReceiptsReply` (protobuf via gRPC) |

---

## Key Files

| File | Role |
|------|------|
| `execution/notifications/` | Types, interfaces, this README |
| `execution/execmodule/notification_dispatcher.go` | Dispatcher: orchestrates dispatch of all notification types |
| `execution/notifications/accumulation.go` | Accumulation: groups Accumulator + RecentReceipts |
| `execution/notifications/accumulator.go` | Accumulator: state change collection (native types) |
| `execution/notifications/batch.go` | Final account values for incomplete output batches |
| `execution/notifications/recent_receipts.go` | RecentReceipts cache |
| `node/shards/events.go` | Events pub/sub (header, log, receipt channels) |
| `execution/stagedsync/stage_finish.go` | NotifyNewHeaders (header RLP dispatch) |
| `node/privateapi/ethbackend.go` | gRPC boundary: native → protobuf conversion |
| `node/privateapi/logsfilter.go` | Log filter aggregation |
| `node/privateapi/receiptsfilter.go` | Receipt filter aggregation |
| `db/kv/remotedbserver/remotedbserver.go` | KvServer: StateChangeConsumer + gRPC streaming |
| `rpc/rpchelper/filters.go` | RPC daemon subscription management |
| `txnprovider/txpool/fetch.go` | TxPool state change consumer |
