# debug_subscribe("executionWitnesses"): push fresh witnesses over WS (PR 2 of 3)

## Overview

Zilkworm currently pulls witnesses: PR 1 (z6m #114) moved it from polling to a WS `newHeads` watermark + request over the same socket. This PR gives erigon the push half: every witness the eager cache builder completes is pushed to `debug_subscribe("executionWitnesses")` subscribers as a JSON-RPC subscription notification, eliminating the last request round trip at tip. PR 3 (z6m) consumes it with `PushWithFallback`.

Semantics are **fresh-only and stateless**: a subscriber receives every witness completed after it subscribed, nothing else. Catch-up stays request/response (`debug_executionWitness`) over the same connection. Erigon keeps zero per-subscriber state beyond one buffered channel.

Stacked on the eager witness cache branch `awskii/witness-cache` (PR #22384, tip `5497dd9a76`): the cache already builds every canonical head eagerly (including reorged-head repopulation) and stores each witness as pre-marshaled JSON (`ExecutionWitnessResult{cachedJSON}`), so the push path reuses those exact bytes — one encode per witness, zero marshal work per subscriber.

## Context (from discovery)

- Worktree/branch: `~/org/wrk/wt/witness-push`, branch `awskii/witness-push`, created from `awskii/witness-cache` @ `5497dd9a76`.
- Publish site: `rpc/jsonrpc/witness_cache_builder.go:282-288` — after a successful build: `enc, err := result.MarshalFastJSON()` (with a real error branch — preserve it) then `api.witnessCache.Add(hash, &ExecutionWitnessResult{cachedJSON: enc})`; `num`, `hash`, `enc` all in scope. The builder is the ONLY production caller of `.Add`.
- **The cache is the single object shared by both witness API impls**: constructed in `NewWitnessCacheBuilderAPI` (`witness_cache_builder.go:144`), assigned to the builder impl, returned to `node/eth/backend.go:1205`, passed through `APIList` (`daemon.go:46-58`) onto the serve-side `debugImpl.witnessCache` (`debug_api.go:88`). The feed rides ON the cache (see Solution Overview) so no signature anywhere changes — `APIList`'s other callers (`cmd/rpcdaemon/main.go:59`, `cmd/mcp/main.go:255`) stay untouched.
- Subscription machinery is namespace-generic: any exported method shaped `func(ctx, ...) (*rpc.Subscription, error)` on a registered API is callable as `debug_subscribe("<methodName>", ...)` (`rpc/service.go:276` `isPubSub`, `rpc/handler.go:603` `handleSubscribe`). Reflection runs on the concrete `*DebugAPIImpl`, so the `PrivateDebugAPI` interface at `daemon.go:107` does NOT need the new method added. Pump pattern to copy: `subscribeRPC` in `rpc/jsonrpc/eth_filters.go:153-197` — note it selects only on the feed channel and `rpcSub.Err()`; this plan deliberately adds a `ctx.Done()` case.
- Test seam is real, not faked: `rpc.ContextWithNotifier` (`rpc/subscription.go:83`) and `rpc.NewLocalNotifier(namespace, resc, closec)` (`rpc/subscription.go:94-109`) are exported — tests inject a local notifier via context and read notifications off `resc`. `LocalNotifier.Notify` blocks on `resc <-`, so buffer `resc` or drain concurrently.
- Existing wiring tests: `rpc/jsonrpc/witness_cache_wiring_test.go` (enabled-path construction needs a non-nil `&httpcfg.HttpCfg{WitnessCacheBlocks: N}`).
- Existing builder metrics (`witnessCacheBuild*`) stay untouched; the no-new-metrics rule applies to the feed only. The cache gate flag (`--witness.cache.blocks` family, `backend.go:1190`) is the only enable switch — no new flags.

## Development Approach

- **Testing approach: TDD (red → green → refactor)** per erigon CLAUDE.md — every task writes its failing tests first and confirms they fail for the right reason. No `t.Skip` in any form.
- Setup (before Task 1; already done if this worktree exists): `git -C ~/org/wrk/erigon worktree add ~/org/wrk/wt/witness-push -b awskii/witness-push awskii/witness-cache`.
- **KISS is a hard requirement**: no interfaces, no new flags, no new metrics, no config surface. If a checkbox seems to need one of those, the design is being misread — stop and re-read Technical Details.
- Comment policy (erigon CLAUDE.md): default no comments; at most one short sentence for a non-obvious invariant (drop-oldest rationale). No PR/issue references, no narration.
- Commit messages: `rpc/jsonrpc: <what>`.
- Every task ends with `go test ./rpc/jsonrpc/ -run <TaskPattern> -count=1` green (use `-race` where the checklist says so); `make lint` gates each commit and is non-deterministic — rerun until clean.
- New files carry the repo's standard license header with year 2026.
- Update this plan when scope changes.

## Testing Strategy

- **Unit tests**: required per task, colocated (`witness_feed_test.go`, extensions to `witness_cache_builder_test.go`, new subscription test file). Feed and pump tests are pure in-memory — no sockets, no server harness.
- **Pump testability**: through the real notifier — `ctx = rpc.ContextWithNotifier(ctx, rpc.NewLocalNotifier("debug", resc, closec))`, call `ExecutionWitnesses`, read notifications off a buffered `resc`. No closures, no fakes, no interfaces.
- **E2E**: manual, post-completion — `wscat` against an embedded node started with the cache flag (see Post-Completion).

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix; blockers with ⚠️
- keep plan in sync with actual work

## Solution Overview

Three small pieces, all in `rpc/jsonrpc`:

1. **`witnessFeed`** — plain struct (mutex, `map[uint64]chan witnessPush`, id counter). Non-blocking fan-out: per-subscriber channel cap 4; on full, drain one then send (drop-oldest, keep-newest — tip proving wants the freshest, and anything dropped is servable by request). Rate-limited `log.Debug` on drops.
2. **Feed rides on the cache** — the feed is a field of the cache object created in `newWitnessResultCache` (if `witnessResultCache` is still a bare LRU type alias, convert it to a small struct embedding the LRU pointer plus the feed; embedding preserves `.Add/.Get/.Len` call sites). Cache non-nil ⇔ feed non-nil; nothing else is plumbed anywhere. The builder publishes `(num, hash, enc)` right after `Add`, sharing the pre-marshaled bytes.
3. **`DebugAPIImpl.ExecutionWitnesses`** — becomes `debug_subscribe("executionWitnesses", {encoding?})` via the generic reflection; validates opts, errors when `api.witnessCache == nil` (standalone rpcdaemon or cache flag off — mirror the nil-guard pattern already in `debug_execution_witness.go`) with a message naming the cache flag; pumps feed → `notifier.Notify` until `rpcSub.Err()` or `notifier.Closed()`, then unsubscribes. (Not `ctx.Done()`: this fork's `startCallProc` cancels the subscribe call's ctx the instant the method returns, so a ctx.Done() case would kill the pump before the first push — see Task 3.)
4. **Notification result** — `{blockNumber, blockHash, witness}` with `witness` the raw cached JSON. Reorgs need no new code: the builder's existing reorged-head repopulation re-publishes the same height with the new hash; replacement is the subscriber's contract (PR 3).

Ordering is the builder loop's build order — single producer, no cross-subscriber coordination.

## Technical Details

```go
type witnessPush struct {
    num  uint64
    hash common.Hash
    json json.RawMessage
}
// subscribe() (uint64, <-chan witnessPush) ; unsubscribe(id uint64) ; publish(p witnessPush)
```

- Channel cap: `const witnessFeedBuffer = 4`. Overflow: `select` send; on default, drop one from the channel, send the new one (both non-blocking; if the drain races empty, plain send succeeds).
- Opts: `type WitnessSubscriptionOpts struct{ Encoding string }`, taken as a POINTER (`opts *WitnessSubscriptionOpts`) so an absent second param arrives as nil — nil or `Encoding` of `""`/`"json"` accepted; anything else → `fmt.Errorf("unsupported witness encoding %q (supported: json)", ...)`. Reserved slot for `rlp`.
- Notification result:
```go
type WitnessNotification struct {
    BlockNumber hexutil.Uint64  `json:"blockNumber"`
    BlockHash   common.Hash     `json:"blockHash"`
    Witness     json.RawMessage `json:"witness"`
}
```
- Nil-cache error text mentions the witness-cache flag (copy its exact name from the branch's flag definition) and that the subscription is embedded-only.
- Feed construction: inside `newWitnessResultCache` only. Because the cache instance already crosses `NewWitnessCacheBuilderAPI` → `backend.go:1205` → `APIList` → `debugImpl.witnessCache`, both impls share the feed with ZERO signature changes — `NewWitnessCacheBuilderAPI` arity, `APIList` params, and the `cmd/rpcdaemon`/`cmd/mcp` call sites all stay byte-identical.

### Wire contract (reference for PR 3)

Subscribe:
```json
{"jsonrpc":"2.0","id":1,"method":"debug_subscribe","params":["executionWitnesses",{"encoding":"json"}]}
```
Notification:
```json
{"jsonrpc":"2.0","method":"debug_subscription","params":{"subscription":"0x…","result":{"blockNumber":"0x1846b3e","blockHash":"0x…","witness":{"state":["0x…"],"codes":["0x…"],"keys":["0x…"],"headers":["0x…"]}}}}
```
Behind-tip catch-up and any gap (drop, reorg, cache miss) → plain `debug_executionWitness` request on the same connection.

## What Goes Where

- **Implementation Steps**: all code + tests in this repo/branch.
- **Post-Completion**: manual e2e, stacked-PR creation, PR 3 pointer.

## Implementation Steps

### Task 1: `witnessFeed` fan-out (TDD)

**Files:**
- Create: `rpc/jsonrpc/witness_feed.go`
- Create: `rpc/jsonrpc/witness_feed_test.go`

- [x] write failing tests: publish reaches two concurrent subscribers; unsubscribe stops delivery and is idempotent; publish with zero subscribers is a no-op; overflow — publish 6 into an unread cap-4 channel, assert the reader gets the newest 4 (oldest 2 dropped); concurrent publish/subscribe/unsubscribe under `-race`
- [x] confirm the tests fail for the right reason (type/behavior missing), then implement `witnessFeed` per Technical Details (plain struct, cap-4 channels, drain-one-then-send overflow, rate-limited `log.Debug` on drop)
- [x] refactor pass with tests green
- [x] run `go test ./rpc/jsonrpc/ -run TestWitnessFeed -count=1 -race`; `make lint` until clean

### Task 2: feed rides on the cache; builder publishes after insert (TDD)

**Files:**
- Modify: `rpc/jsonrpc/witness_cache.go` (cache struct gains the feed; convert the LRU type alias to an embedding struct if needed)
- Modify: `rpc/jsonrpc/witness_cache_builder.go`
- Modify: `rpc/jsonrpc/witness_cache_builder_test.go`

- [x] write failing test: the build-success path (extract the existing store tail into a small `storeWitness(num, hash, enc)` helper on the API if that is the cheapest seam) both inserts into the cache and delivers the IDENTICAL `json.RawMessage` bytes to a subscriber on the cache's feed (`bytes.Equal` on the cached and pushed payloads)
- [x] implement: `newWitnessResultCache` constructs the feed as a cache field (embed the LRU pointer if the type is currently an alias — `.Add/.Get/.Len` call sites must not change); builder publishes `(num, hash, enc)` immediately after `witnessCache.Add` — cache non-nil implies feed non-nil, no nil-feed branch
- [x] refactor pass; confirm no behavior change to existing builder/cache tests and ZERO edits outside `rpc/jsonrpc/`
- [x] run `go test ./rpc/jsonrpc/ -run 'TestWitness' -count=1`; `make lint` until clean

### Task 3: `debug_subscribe("executionWitnesses")` endpoint + pump (TDD)

**Files:**
- Create: `rpc/jsonrpc/witness_subscription.go`
- Create: `rpc/jsonrpc/witness_subscription_test.go`

- [x] write failing tests using the real notifier seam — `ctx = rpc.ContextWithNotifier(ctx, rpc.NewLocalNotifier("debug", resc, closec))` with a buffered `resc` (LocalNotifier.Notify blocks): opts validation (nil pointer and `"json"` accepted; `"rlp"`/garbage rejected with the exact "unsupported witness encoding" error); nil-cache call returns the actionable embedded-only error naming the cache flag; pump delivers `WitnessNotification` with verbatim `witness` bytes for each feed publish; pump exits and unsubscribes when the connection closes (`notifier.Closed()`)
- [x] implement `ExecutionWitnesses(ctx context.Context, opts *WitnessSubscriptionOpts) (*rpc.Subscription, error)` following the `subscribeRPC` pattern (`eth_filters.go:153-197`). Pump select exits on `rpcSub.Err()` or `notifier.Closed()` — NOT `ctx.Done()`: the plan's ctx.Done() idea is wrong in this fork because `startCallProc` (`rpc/handler.go:434`) cancels the subscribe call's ctx via `defer cancel()` the instant the method returns, which would kill the pump before any witness is pushed. The wire test below caught this; `rpcSub.Err()` (closed on both client-unsubscribe and connection-drop) and `notifier.Closed()` (the only lever the LocalNotifier harness exposes) are the correct signals.
- [x] rpc package exposes an in-process server+client path (`rpc.NewServer` + `RegisterName` + `rpc.DialInProc`, per `eth_simulation_test.go:565`): added `TestWitnessSubscriptionWireDispatch` asserting `debug_subscribe("executionWitnesses")` dispatches to the real method on a registered `*DebugAPIImpl` and delivers a builder-published witness verbatim over the wire
- [x] refactor pass
- [x] run `go test ./rpc/jsonrpc/ -run TestWitnessSubscription -count=1 -race`; `make lint` until clean

### Task 4: wiring test — one shared feed iff the cache is enabled (TDD)

**Files:**
- Modify: `rpc/jsonrpc/witness_cache_wiring_test.go`

- [x] write failing wiring test (enabled path needs a non-nil `&httpcfg.HttpCfg{WitnessCacheBlocks: N}` or `newWitnessResultCache` nil-derefs): with the cache enabled, the builder's publish reaches a subscriber obtained through `debugImpl.witnessCache` — same feed instance end to end; with the cache disabled, `ExecutionWitnesses` returns the embedded-only error
- [x] make it pass — expected to require NO production edits beyond Tasks 2-3 (the cache carries the feed); if a production edit turns out to be needed, stop and re-check Task 2's construction site before adding plumbing
- [x] run `go test ./rpc/jsonrpc/ -run 'TestWitness' -count=1`; `make lint` until clean

### Task 5: verify acceptance criteria

- [ ] fresh-only stateless semantics confirmed against the Overview (no replay, no per-subscriber state beyond the channel)
- [ ] `git diff --stat awskii/witness-cache` touches ONLY `rpc/jsonrpc/` files plus this plan; `git diff awskii/witness-cache -- cmd/ node/` is EMPTY; **zero new flags, zero new metric series** (grep the diff for `metrics.` and flag registrations)
- [ ] full package: `go test ./rpc/jsonrpc/ -count=1 -timeout 30m`
- [ ] `make lint` repeatedly until clean twice in a row
- [ ] re-read the wire contract section — request/notification shapes match the implementation exactly (field names, casing)

### Task 6: wrap-up

- [ ] confirm the wire-contract section of this plan matches final code (PR 3 consumes it verbatim)
- [ ] update checkboxes to final state; move this plan to `docs/plans/completed/`

## Post-Completion

**Manual e2e**: start an embedded node with the witness-cache flag on a dev chain (or the erigon-ephemeral skill against a small datadir), then
`wscat -c ws://localhost:8545 -x '{"jsonrpc":"2.0","id":1,"method":"debug_subscribe","params":["executionWitnesses"]}'` — expect a notification per new block; kill/restart wscat to confirm stateless fresh-only behavior; run a second subscriber to confirm fan-out.

**PR**: stacked — base branch `awskii/witness-cache`, body: problem paragraph (zilkworm pulls witnesses; PR 1 gave it the WS transport and watermark; this adds the node-side push from the eager cache, removing the last per-block round trip at tip), then `## Changes`; note "stacked on #22384"; terse, no Testing section. When #22384 merges, retarget base to `main` and rebase.

**Series**: PR 3 (z6m) — `WitnessSource::PushWithFallback` consuming this subscription: push buffer keyed by height, replace on same-height re-push (reorg), timeout → request fallback; `rlp` encoding lands as a later erigon+z6m pair using the reserved `encoding` param.
