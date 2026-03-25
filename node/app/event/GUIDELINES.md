# Event Bus — Developer Guidelines

## Overview

The event bus provides typed, reflection-based pub/sub for inter-component communication. Events are dispatched by argument type — subscribers register handler functions whose parameter types determine which events they receive.

## Migration Strategy: Single Domain First, Hierarchical Later

**All componentization work targets a single shared domain for L1.** This is a deliberate architectural decision that guides all other choices.

**Phase 1 (now → L1 working):** All components share the root domain. Events flow freely between Storage, Downloader, Sync, Execution, TxPool, RPC, and plugins. This is simple, testable, and sufficient for a fully event-driven L1 node.

**Phase 2 (L1/L2 combined mode):** When the combined node arrives, we add hierarchical domains — L1 components move into a child L1 domain, L2 into a child L2 domain, shared base stays in root. This requires cross-domain event propagation (see "Future" section below).

**Why this order matters:**
- Component code doesn't change between phases — components publish/subscribe events without knowing which domain they're in
- The single domain validates the event-driven architecture with a working L1 before adding domain hierarchy complexity
- The adaptation is isolated to event bus wiring and NodeBuilder, not component internals
- We get a working system sooner

**What this means for developers right now:**
- Design components as if there's one domain (because there is)
- Don't pass domain references into component logic — let the framework handle it
- Define event types as plain structs in a shared package — they'll work across domain boundaries when hierarchical domains arrive
- Don't build domain-aware routing into components — that's the framework's job

## Domain Architecture

**Events are scoped per-domain.** Components in different domains CANNOT communicate via events. This is the single most important architectural decision when designing the component hierarchy.

**Default: use the root domain for all components.** Unless you have a specific reason to isolate a subsystem, all components should share one domain so events flow freely between them.

```
CORRECT — all components share the root domain:

  root domain
  ├── Storage          ──publishes──▶ SnapshotFilesCreated
  ├── Downloader       ◀─receives────
  ├── Sync
  ├── Execution
  ├── TxPool
  ├── RPC
  └── plugins/snapshot-manager

WRONG — components in separate domains can't communicate:

  domain-a                    domain-b
  ├── Storage ──publishes──▶ │ ✗ Downloader never receives
  └── Sync                   └── TxPool
```

**When to create a separate domain:**
- Never, unless you are building a truly isolated subsystem with NO event dependencies on other components
- A separate domain means a separate worker pool, separate service bus, and complete event isolation
- If you think you need a separate domain, you probably don't — use the root domain and use event type routing for isolation instead

**When separate domains ARE appropriate:**
- Test harnesses that need isolation from production components
- Completely independent subsystems (e.g., a standalone monitoring agent)
- Load isolation: a subsystem that would flood the shared worker pool (use `WithExecPoolSize` to create a domain with its own pool, but note events still won't cross domain boundaries)

**Future: hierarchical event propagation (needed for L1/L2 combined mode).**

The combined L1/L2 node ([proposal](https://github.com/erigontech/erigon-documents/tree/master/cocoon/pocs-and-proposals/l1-l2-node)) requires three domain levels:

```
root domain (shared base)
├── Storage, Downloader, P2P, RPC (chain-ID router)
│
├── L1 domain
│   ├── L1 Sync, L1 Execution, L1 TxPool, Caplin
│   └── (L1 internal events stay here)
│
└── L2 domain
    ├── L2 Sync, L2 Execution, L2 TxPool, RollupDriver
    └── (L2 internal events stay here)
```

L1 and L2 need isolation from each other (separate state machines, separate block
processing) but both need events from the shared base (e.g. `SnapshotDownloadComplete`),
and L2 needs to subscribe to specific L1 events (e.g. `L1BlockFinalized` → triggers
L2 derivation).

This requires three capabilities not present today:
1. **Child → parent subscription**: child domains can subscribe to parent domain events
2. **Cross-domain directed events**: L1 publishes `L1BlockFinalized`, L2 subscribes via root
3. **No upward propagation by default**: child domain internal events don't leak to parent

This is NOT implemented today. It is needed before Phase 3 of the L1/L2 combined
mode work (componentization Wave 5, PR 21 — `L2 NodeBuilder mode`). For L1-only
operation (the current default), the single root domain is sufficient.

## Event Ordering

| Handler Type | Ordering | Use When |
|-------------|----------|----------|
| `Subscribe(fn)` | **Totally ordered** within a single `Publish` call. Handlers execute synchronously under the bus lock in registration order. | You need deterministic ordering and can guarantee the handler is fast. |
| `SubscribeAsync(fn)` | **No ordering guarantee.** Handlers are dispatched to the worker pool and may execute in any order. | Default. The handler may be slow or do I/O. |

**Events are scoped per-domain.** See "Domain Architecture" above.

## Rules for Event Handlers

### 1. Never block in an async handler

Async handlers run in a shared worker pool. Blocking one starves others:

```go
// BAD — blocks a worker thread
func (p *MyProvider) HandleEvent(evt SomeEvent) {
    result := expensiveRPC(evt.Data)  // blocks for seconds
    p.process(result)
}

// GOOD — offload blocking work
func (p *MyProvider) HandleEvent(evt SomeEvent) {
    go func() {
        result := expensiveRPC(evt.Data)
        p.process(result)
    }()
}
```

### 2. Keep handlers idempotent

Events may be delivered more than once in edge cases (component restart, resubscription). Design handlers to be safe on replay.

### 3. Don't publish from within a sync handler

Sync handlers hold the bus lock. Publishing from inside a sync handler will deadlock:

```go
// BAD — deadlock (Publish tries to acquire the same lock)
bus.Subscribe(func(evt FileCreated) {
    bus.Publish(DownloadComplete{})  // DEADLOCK
})

// GOOD — publish asynchronously
bus.Subscribe(func(evt FileCreated) {
    go bus.Publish(DownloadComplete{})
})
```

### 4. Handle panics gracefully

The bus catches panics in handlers and logs them. Your handler won't crash the bus, but it also won't complete its work. Use `defer recover()` for critical cleanup:

```go
func (p *MyProvider) HandleEvent(evt SomeEvent) {
    defer func() {
        if r := recover(); r != nil {
            p.logger.Error("handler panic", "err", r)
        }
    }()
    p.riskyOperation(evt)
}
```

## Preserving Atomicity

Events are fire-and-forget — there's no built-in transaction or acknowledgement. If you need atomicity:

### Pattern: Event + Confirmation

```go
// Publisher
type DownloadRequested struct {
    ID    string
    Items []DownloadItem
}

// Subscriber publishes result when done
func (p *Downloader) HandleDownloadRequested(evt DownloadRequested) {
    err := p.download(evt.Items)
    if err != nil {
        bus.Publish(DownloadFailed{ID: evt.ID, Err: err})
    } else {
        bus.Publish(DownloadComplete{ID: evt.ID})
    }
}
```

### Pattern: Saga for multi-step flows

For flows that span multiple components (e.g., "download snapshot, verify integrity, update index"), use a saga coordinator:

```go
type SnapshotSaga struct {
    id    string
    state string // "downloading", "verifying", "indexing", "complete"
}

func (s *SnapshotSaga) HandleDownloadComplete(evt DownloadComplete) {
    if evt.ID == s.id {
        s.state = "verifying"
        bus.Publish(VerifyRequested{ID: s.id, Path: evt.Path})
    }
}
```

## Dead Letters

Events published with no subscribers return `count == 0` from `Publish()`. Currently these are silently dropped. TODO: add dead letter logging (see downloader README).

## Performance

- **10,000 events** delivered reliably in <10ms (TestEventLoadCompleteness)
- **Async handlers** add ~microsecond overhead per dispatch (goroutine spawn)
- **Worker pool** size defaults to `NumCPU * POOL_LOAD_FACTOR` — monitor queue size for pool exhaustion
- **Publish is O(subscribers)** — scales linearly with handler count

## Subscribing

```go
// Subscribe a specific method
bus.SubscribeAsync(myComponent.HandleFileCreated)

// Via ManagedEventBus — auto-discovers ALL methods with inputs and no outputs
managedBus.Register(myComponent)

// Unsubscribe
bus.Unsubscribe(myComponent.HandleFileCreated)
```

## Defining Events

Events are plain Go structs. Use value types (not pointers) for immutability:

```go
// Good — immutable value type
type FileCreated struct {
    Path string
    Size uint64
}

// Avoid — mutable pointer, subscribers could modify shared state
type FileCreated struct {
    Data *[]byte  // don't do this
}
```

## Testing

Use `event.NewEventBus(pool)` with a test pool for unit tests:

```go
pool := &testPool{} // implements util.ExecPool
bus := event.NewEventBus(pool)

bus.SubscribeAsync(handler)
bus.Publish(MyEvent{})
bus.WaitAsync() // blocks until all async handlers complete

// Assert handler was called
```
