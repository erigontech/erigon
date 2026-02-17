# Fix state change subscription race condition causing test timeout

## Summary

Fix a race condition in the direct (in-process) state change subscription that caused `TestExecutionSpecBlockchain` to hang indefinitely on CI, resulting in a 1-hour timeout in the `tests-mac-linux (macos-15)` job.

## Root Cause

The `StateDiffClientDirect.StateChanges()` method in `node/direct/state_diff_client.go` had a **timing hole** between when the function returned (indicating readiness to the caller) and when the actual subscription to state change events was registered on the server side.

The existing code acknowledged this issue with a comment:
```
// there is a timing hole on startup which breaks tests
// really we need a subscribed event so the subscriber
// can ensure that the subscription is complete before it
// starts processing - this just shortens the gap
```

The previous mitigation (`started` channel) only ensured the goroutine was launched, not that the subscription was active. With `fcuBackgroundCommit=true` (the mock test default), state change notifications are published by a background goroutine after `UpdateForkChoice` returns `Success`. This widened the race window enough to occasionally lose events:

1. `insertPoSBlocks` calls `StateChanges()` → goroutine starts, caller unblocks
2. Goroutine is **preempted before calling `Sub()`** on the server
3. `UpdateForkChoice` runs the pipeline successfully, returns `Success`
4. Background goroutine publishes state changes via `Pub()` → **no subscribers yet** → events lost
5. Goroutine finally calls `Sub()`, but the events are already gone
6. `insertPoSBlocks` blocks forever on `stream.Recv()` waiting for events that will never arrive

## Fix

Two changes:

### 1. Close the state change subscription timing hole (`node/direct/state_diff_client.go` + `db/kv/remotedbserver/remotedbserver.go`)

- Added a `StateChangeSubscriber` interface with a `NotifySubscribed()` method
- `KvServer.StateChanges` now calls `NotifySubscribed()` **after** `Sub()` registers the subscription
- `StateDiffStreamS` implements this interface via a `subscribed` channel
- `StateDiffClientDirect.StateChanges()` blocks on the `subscribed` channel instead of the old `started` channel, ensuring the subscription is fully active before returning

### 2. Fix goroutine leak in `node/eth/backend.go`

- The goroutine at `backend.go:970` that processes mined/pending blocks only exited on `QuitPoWMining`
- Added `case <-ctx.Done()` to also exit when the backend context is cancelled
- This prevents goroutine leaks in tests that use `eth.New()` (engine API tests in the same binary), which were visible as 59-minute-old goroutines in the CI dump

## Verification

- `make erigon integration` builds successfully
- `make lint` passes cleanly (run twice)
- `TestExecutionSpecBlockchain` (the timed-out test) passes
- Specific PUSH0/Berlin subtest passes with `-count=3`
- `TestEngineApi*` tests pass (exercises the `eth.New()` goroutine fix)
- `db/kv/remotedbserver` package tests pass
- `execution/stagedsync` package tests pass
