# Event Bus — Developer Guidelines

## Overview

The event bus provides typed, reflection-based pub/sub for inter-component communication. Events are dispatched by argument type — subscribers register handler functions whose parameter types determine which events they receive.

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

**Future: hierarchical event propagation.** If a use case arises where child domains need to receive parent events (or vice versa), cross-domain event bridging or hierarchical propagation would need to be added to the framework. This is not implemented today and should not be designed until there is a concrete use case driving the requirements.

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
