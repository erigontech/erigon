# Notification Flows

This package defines the types and interfaces for Erigon's execution notification
system. Notifications carry state changes, headers, logs, and receipts from the
execution pipeline to downstream consumers (TxPool, RPC daemon, P2P).

## Architecture

Two paths exist for each notification type:

- **Local path**: in-process consumers receive native Go types directly. No protobuf.
- **Remote path**: gRPC consumers receive protobuf messages. Conversion from native
  types happens at the gRPC boundary (`node/privateapi/`), not in the execution layer.

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
  → Accumulator.SendAndReset(ctx, consumers, baseFee, blobFee, gasLimit, finalized)
```

### Local Path (TxPool)

```
BlockBatchConsumer.OnNewBlock(BlockBatchNotification)

BlockBatchNotification contains:
  - StateVersionID      uint64
  - Changes             []StateChange  (native types)
    - Direction          (Forward/Unwind)
    - BlockHeight        uint64
    - Txs                [][]byte       ← TxPool's primary interest
    - Changes            []AccountChange (TxPool ignores these)
  - PendingBlockBaseFee  uint64
  - PendingBlobFee       uint64
  - BlockGasLimit        uint64
  - FinalizedBlock       uint64
```

TxPool only reads: `Direction`, `Txs` (to track mined/unwound transactions),
`PendingBlockBaseFee`, `BlockGasLimit`, `FinalizedBlock`. It ignores all
account/storage change data.

### Remote Path (gRPC → remote rpcdaemon)

```
StateChangeConsumer.SendStateChanges(StateChangeBatch)  [protobuf]
  → KvServer (db/kv/remotedbserver/)
    → StateChangePubSub.Pub()
      → gRPC stream to remote rpcdaemon
        → kvcache.Coherent.OnNewBlock()  (updates remote state cache)
```

Protobuf `StateChangeBatch` contains the full `AccountChange` list with
`H160`/`H256` types, needed by remote `kvcache` to update its state cache.
In embedded mode, `execmodule.Cache` replaces `kvcache` and its `OnNewBlock`
is a no-op.

### Consumer Summary

| Consumer | Mode | Reads | Ignores |
|----------|------|-------|---------|
| TxPool | local | Txs, Direction, fees | AccountChange, StorageChange |
| kvcache.Coherent | remote | AccountChange, StorageChange, Code | Txs |
| execmodule.Cache | local | nothing (no-op) | everything |

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
  → RecentReceipts.NotifyLogs(events, from, to, isUnwind)
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
  → RecentReceipts.NotifyReceipts(events, from, to, isUnwind)
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
| `execution/notifications/recent_receipts.go` | RecentReceipts cache |
| `node/shards/events.go` | Events pub/sub (header, log, receipt channels) |
| `execution/stagedsync/stage_finish.go` | NotifyNewHeaders (header RLP dispatch) |
| `node/privateapi/ethbackend.go` | gRPC boundary: native → protobuf conversion |
| `node/privateapi/logsfilter.go` | Log filter aggregation |
| `node/privateapi/receiptsfilter.go` | Receipt filter aggregation |
| `db/kv/remotedbserver/remotedbserver.go` | KvServer: StateChangeConsumer + gRPC streaming |
| `rpc/rpchelper/filters.go` | RPC daemon subscription management |
| `txnprovider/txpool/fetch.go` | TxPool state change consumer |
