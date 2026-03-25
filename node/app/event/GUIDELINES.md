# Event Bus — Developer Guidelines

## Overview

The event bus provides typed, reflection-based pub/sub for inter-component communication. Events are dispatched by argument type — subscribers register handler functions whose parameter types determine which events they receive.

## Event Ordering

| Handler Type | Ordering | Use When |
|-------------|----------|----------|
| `Subscribe(fn)` | **Totally ordered** within a single `Publish` call. Handlers execute synchronously under the bus lock in registration order. | You need deterministic ordering and can guarantee the handler is fast. |
| `SubscribeAsync(fn)` | **No ordering guarantee.** Handlers are dispatched to the worker pool and may execute in any order. | Default. The handler may be slow or do I/O. |

**Events are scoped per-domain.** Each `ComponentDomain` has its own `ServiceBus`. Events published on one domain's bus do NOT propagate to parent or child domains.

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
