# Commitment v4: wire the branch cache and adapt the warmupper

Plan, 2026-09-22. Worktree `wt/v4-cache-warmup`, branch `awskii/commitment-v4-cache-warmup`, off
`awskii/commitment-drop-dead-surface` @ `93d9241576a`. `origin/main` has no `execution/commitment/v4/`,
so this cannot branch from the default branch.
Revised after review, 2026-09-22.

## Overview

v4 keys commitment records as `tag || [addrHash 32B] || packPath || nibbleCount` (`execution/commitment/v4/key.go:99`).
Two subsystems still address records by the V1/V2 compact key shape and parse the V2 record body, so under
v4 both run and neither does anything:

- **`BranchCache` is mis-tiered.** Routing is self-consistent for `Put`/`Get`, so no wrong record is
  returned, but every v4 key lands in the wrong tier, the hottest account levels fall out of the trunk
  entirely, and `PinEntry` routes v4 account keys somewhere `lookup` will never look.
- **The warmupper is dead twice over.** v4 drops the warmuper on the floor, and if it were wired it would
  build keys no v4 datadir holds and parse fields the v4 record does not have.

This plan fixes the addressing in both, and leaves a counter behind so neither can silently rot again.
It changes no record format and no root.

### What this plan does not do

Deferred to Task 5, after measurement — listed here so they are not smuggled in early:

- a decoded-node cache tier (rejected outright: see Solution Overview B)
- warming leaves specifically
- a cold-start seed pass over the top of the trie
- removing the `branchBuf` copy on a cache hit, and the per-child ext unpack

**Explicitly out of scope:** the trunk preload is a *third* subsystem equally dead under v4 —
`execution/commitment/preload_parallel.go:50` builds `nibbles.HexToCompact` keys and
`preload_ranges.go:18,25,26` scans compact ranges. Nothing here fixes it and the Task 4 counter does not
cover it. File it in `docs/backlog/` during Task 7 rather than letting "neither can silently rot again"
read as covering three subsystems when it covers two.

## Context (from discovery)

Every `file:line` below was read in this worktree on 2026-09-22 and re-verified in review. Line numbers rot
— re-verify before citing.

**Files involved:**

| file | role |
|---|---|
| `execution/commitment/branch_cache.go` | the cache; `trunkSlot:277`, `storageRoute:310`, `ContractHashFromPrefix:350`, `storageNibbles:368`, `isRootPrefix:427`, `lookup:431`, `PinEntry:508`, `Get:544`, `Put:563`, `Stats:639` |
| `execution/commitment/warmuper.go` | `WarmupConfig:37`, worker pool, arena barrier, `NewWarmuper:80`, `warmupKey:144` with the V2 parse inline at `:165-200`, `startTime:64` |
| `execution/commitment/hex_patricia_hashed.go` | constructs the Warmuper at `:2622-2628` behind `if warmup.Enabled`; new home for the V2 parse |
| `execution/exec/bal_commitment_warmup.go` | **second, out-of-package** `NewWarmuper` site at `:134` |
| `execution/commitment/commitment.go` | `WarmKey` call sites `:1805` (ModeDirect) and `:1883` (ModeUpdate); LCP dedupe `:1876-1884`; `IsCommitmentStateKey:167` |
| `execution/commitment/v4/trie.go` | `Process:132`; `:152` passes `nil` where the warmuper belongs; `runScheduledPhases:165` |
| `execution/commitment/v4/record.go` | `layout():375`, `tree():59`, `extAt:157`, `slotAt:116`, header flags `:28-32`, self-ext root-only `:257` |
| `execution/commitment/v4/key.go` | `nodeKey:99` (allocates at `:105`), `AccountNodeKey`, `StorageNodeKey`, `tagState:27` |
| `execution/commitment/v4/path.go` | `packPath:26` — allocates whenever `cap(dst) < packed` |
| `execution/commitment/v4/unfold.go` | `:53,60` build keys with `dst=nil`; `:109` `decodeExtension` allocates per child |
| `db/state/execctx/domain_shared.go` | cache read `:1473-1482` |
| `db/state/aggregator.go` | cache fill `cacheLatestBranch:2725`; construction `:427-429` |
| `execution/commitment/commitmentdb/commitment_context.go` | `TrieContext.Branch:1073`, `branchBuf` copy `:1092`, `WarmupConfig` assembly `:583-614`, `warmupTrieContextFactory:727` |
| `execution/commitment/commitmentdb/reader.go` | `LatestStateReader.Read:68` |

**Three findings this plan rests on:**

1. **The cache is mis-tiered, not broken.** `trunkSlot` (`:277`) switches on `len(prefix)` plus a parity
   flag in bit 4 of byte 0; `ContractHashFromPrefix` (`:354`) reconstructs the account hash by nibble-shift;
   `storageNibbles` (`:373`) derives depth as `2*len(prefix)-64-off`. Against v4 keys:

   | v4 key | routes to today | intended tier |
   |---|---|---|
   | account root `40 00` | `accountTrunk.d2[0]` | `root` |
   | account depth 1-2 | `accountTrunk.d4[packed<<8\|count]` | `d1`/`d2` |
   | account depth 3-6 | tail LRU | `d3`/`d4` |
   | storage root `41 H 00` | storage trunk `d2[0]` | `d0` |
   | storage depth 1-2 | storage trunk `d4` | `d1`/`d2` |

   `Put` (`:563`) and `Get` (`:544`) route identically, so nothing returns a wrong record. Two accidents
   keep it collision-free and **neither is a property the code states**: `ContractHashFromPrefix` takes its
   even branch on tag `0x41`, so `prefix[1:33]` happens to be the v4 addrHash; and the `d4` index
   `packed<<8|count` is injective across depths 1 and 2 only because the count byte differs. A trunk slot
   holds no key (`branchCacheEntry:82-87`), so a future collision returns a wrong record silently.

   **`PinEntry` (`:508`) is a routing asymmetry that is latent, not live.** It routes only through
   `storageRoute(prefix, true, ..)`, which rejects a v4 *account* key (`len < 33`), so the entry lands in
   the tail. But `lookup` (`:442-450`) resolves that same key through `trunkSlot`, and **a trunk miss
   returns immediately without falling through to the tail** — the pinned entry would be unreachable for
   the life of the cache. Nothing produces that case today: the sole production caller
   (`preload_parallel.go:154`) passes `pk.key`, which is `nibbles.HexToCompact(path)` (`:50`), a compact
   key that routes correctly. It becomes live the moment the preload is ported to v4 keys. Route it in
   Task 1 anyway — three lines, and the alternative is a wrong-tier bug that appears in a later change
   with no test to catch it.

   Latent hazard: `storageRoute` (`:310`) gates on `len(prefix) >= 33 && prefix[0]&0x20 == 0`, and
   `0x40&0x20 == 0` — an account node at path depth >= 62 is read as a storage key. Unreachable at real
   fan-out; the tag dispatch removes the gate rather than guarding it.

2. **Warmup is dead twice over.** `v4/trie.go:152` calls `updates.HashSort(ctx, nil, p.add)` — the `warmup`
   parameter is accepted and dropped. If it were wired, `warmuper.go:148` builds
   `HexToCompactInto(hashedKey[:depth])`, a key no v4 datadir holds: the first read misses,
   `len(branchData) < 4` breaks the loop, one burned lookup per key. And `warmupKey:165-200` parses the V2
   record inline (skip the 2-byte touch map, read the bitmap, `skipCellFields`,
   `fieldAccountAddr|fieldStorageAddr`, uvarint extension) — none of those fields exist in v4's
   `flags | childMask | leafMask` layout.

   `commitment_context.go:583-614` already hands v4 a fully formed `WarmupConfig`
   (`MaxDepth = WarmupMaxDepth = 128`, a working `CtxFactory` from `warmupTrieContextFactory:727`) through
   the `default` branch of its type switch. v4 throws it away.

3. **Warmup already populates `BranchCache`; no new plumbing is needed.** `TrieContext.Branch` (`:1073`)
   -> `readDomain:1114` -> `LatestStateReader.Read` (`reader.go:68`) -> `getter.GetLatest` -> `sd.getLatest`
   (`domain_shared.go:1473-1482` reads the cache and sets `WithBranchCache`) ->
   `AggregatorRoTx.cacheLatestBranch` (`aggregator.go:2725`) fills it on a miss. On a hit `getLatest`
   returns early, so a warm entry is never rewritten.

**Background:** `docs/plans/20260921-commitment-v4-architecture.md` (§8 residency, §5 touch phase, §12 order),
`docs/plans/20260921-commitment-v4-trie-and-layout.md`, `execution/commitment/v4/README.md`.

## Development Approach

- **testing approach: TDD.** Red test first, and a red test proves nothing until the failure is the
  *missing fix* — name the assertion that fired and the value it saw. A compile error, a panic before the
  assert, or a typo'd expectation is a false red.
- **Guard tests are mutation-verified by file copy.** Copy the file to the scratchpad, revert the whole
  change, run the whole package, name the `file:line` that went red, copy the file back. Never
  `git checkout --` or `git restore` — a hook wraps both in a dirty tree with `stash push`/`stash pop`,
  and the stash stack is shared with every other worktree (see Constraints).
- Complete each task fully before starting the next; all tests green before moving on.
- Small, focused changes. Backward compatibility with the V1/V2 compact path is mandatory: Tasks 1 and 2
  must leave HPH behaviour bit-identical, and no v4-specific behaviour may enter the format-agnostic loop.
- Update this plan when scope changes: `[x]` on completion, `➕` for new tasks, `⚠️` for blockers.

### Constraints

- **No code comments.** A hook denies any edit adding one to a code file; directives, license headers and
  shebangs pass. Explanation goes in the commit message or PR body.
- **New files get `2026` in the license header**, not the neighbour's year.
- Go naming: no `Factory`/`Provider`/`Manager`/`*Base`. `WarmupKeyFunc`/`WarmupStepFunc` follow the tree's
  existing `TrieFunc` precedent.
- **Stay inside this worktree.** `wt/pc-drop-main` holds the same branch's earlier tip and another session's
  uncommitted work; never `cd` there, and never checkout, switch, rebase, amend or push. The git stash
  stack is shared across every worktree — use a WIP commit to set work aside, never bare `git stash`.
- **`docs/backlog/` is gitignored and lives in the main checkout**, so a copy written here dies with the
  worktree. Task 7's backlog item goes in `~/org/wrk/erigon/docs/backlog/`.
- Commit subject <= 120 chars, subject line only.
- `golangci-lint` is repo-pinned: `go tool -modfile=golangci-lint.mod golangci-lint run`. A stale
  `~/go/bin` binary refuses to run.

## Testing Strategy

- **unit tests**: required in every task, listed as separate checklist items from the implementation.
- **no e2e suite** applies here; this is a library-internal change.
- **package commands**: `go test ./execution/commitment/...`, `go test ./db/state/...`,
  `go test ./execution/exec/...`.
- **the root is the backstop**: `TestLegacyVsHexRoot` and `TestIncrementalRootsAgree` are the only things
  in the tree that catch a root divergence. Run both after Task 3.
- **the anti-rot guard is a test, not a log line**: Task 4's assertion that `RecordsFound > 0` is what
  separates "warmup works" from "warmup reads nothing and returns quietly".

## Solution Overview

**A — the cache gets a tag dispatch, not a wider parity router.** v4 keys self-describe what the current
router reconstructs: byte 0 is the plane tag, the last byte is the depth, and `addrHash` is a plain
`prefix[1:33]` slice. Routing collapses to four lines and the compact path is untouched:

```
0x40  accountTrunk,  depth = prefix[n-1], nibbles = unpack(prefix[1:n-1])
0x41  storage trunk keyed by prefix[1:33], depth = prefix[n-1], nibbles = unpack(prefix[33:n-1])
0x42  not cached  (IsCommitmentStateKey, commitment.go:167, already covers it)
else  today's compact routing, unchanged
```

**All three routing entry points take the dispatch** — `lookup` (`:431`), `store` (via `Put:563`) and
`PinEntry` (`:508`). Routing `Put` and `lookup` alone leaves `PinEntry`'s v4 account keys unreachable, which
is the defect above, not a tier inefficiency.

The dispatch is safe only because no V1/V2 compact prefix can begin `0x40`/`0x41`/`0x42`:
`nibbles.go:55-58` sets only bits `0x20` and `0x10` in the compact flag byte. That precondition is currently
unstated and untested; Task 1 pins it.

**B — cache bytes, not decoded nodes.** Reading child *k* is a popcount plus a slice (`record.slotAt:116`).
A decoded tier saves the popcount and costs the E2 residency trap plus the stale-node invalidation problem
§8 of the architecture doc says v4 does not have. The real per-read cost is three allocations, none of them
the decode:

| per node read | site | disposition |
|---|---|---|
| key allocation | `v4/unfold.go:53,60` pass `dst=nil` **and** `nodeKey:105` calls `packPath(path, nil)`, which allocates whenever `cap(dst) < packed` (`path.go:28-29`) — so threading `dst` into the wrappers alone removes nothing | Task 3; both halves or neither |
| record copy | `commitment_context.go:1092` `branchBuf` — unnecessary on a cache hit, since `Put:563` copies in and `Get:561` returns `entry.data` uncopied | Task 5, after measurement |
| ext unpack per child | `unfold.go:109` -> `decodeExtension` -> `unpackPath(.., nil)` | Task 5, after measurement |

On leaves: under the record design the leaf value lives in the parent record, so there is no separate leaf
fetch. "Warming a leaf" means warming the deepest branch — the one-per-key, non-shareable part. That is a
measurement (Task 5), not a guess.

**C — the warmupper loses its format knowledge.** `warmuper.go` is package `commitment`; `v4` imports
`commitment`, so the v4 parse can never be imported there. Rather than duplicate the worker pool inside v4,
lift both format-dependent halves into `WarmupConfig` (which lives in `warmuper.go:37`, beside its only
consumer):

```go
type WarmupKeyFunc  func(hashedKey []byte, depth int, dst []byte) ([]byte, bool)
type WarmupStepFunc func(record, hashedKey []byte, depth int) (nextDepth int, stop bool)
```

**`Step` takes `hashedKey`, not a precomputed `next` nibble.** The caller cannot know which nibble to follow:
a self-extended record moves the branch point `record[1]` nibbles deeper, and the account-to-storage plane
restart is a v4-only rule that must not enter the shared loop. Both belong to the format, so both live behind
`Step`, which returns the next depth to read or `stop`.

HPH fills the pair with `HexToCompactInto` plus its existing parse, moved out of `warmuper.go` into
`hex_patricia_hashed.go` where the format lives. v4 fills them inside its own `Process` before
`NewWarmuper`. No `commitmentdb` change, no new interface, no new package; `warmupKey` becomes a short
format-agnostic loop that knows only `startDepth`, `maxDepth`, and the two functions.

Rejected alternative: a neutral `execution/commitment/warmup` subpackage. It breaks the cycle too, but moves
a working component and its arena barrier for no gain over two function fields.

## Technical Details

### The v4 warmup step

All accessors already exist and allocate nothing. `depth` is plane-local; `tree()` is the record's own
descent predicate.

```go
if len(record) == 0 { return 0, true }
r := NewRecord(record, localDepth)
if r.isLeafRoot() { return 0, true }
l := r.layout()
if !l.ok { return 0, true }
next := hashedKey[branchPoint]
bit := uint16(1) << next
if l.tree()&bit == 0 { return 0, true }
d := branchPoint + 1
if ext := r.extAt(l, int(next)); len(ext) > 0 { d += int(ext[0]) }
return d, false
```

`tree()` (`record.go:59`) is `child &^ leaf &^ emb`, so one mask test replaces the child-present and
leaf-terminator tests and stops depending on the `emb ⊆ leaf` invariant holding. No `Validate` call — warmup
is best-effort and `layout()` already returns `ok=false` on truncation.

**Four traps, one test each:**

1. **Self-extension is root-only** — `record.go:257` rejects `hdrHasSelfExt` at `depth != 0` — but the root
   is step one of every descent, and a storage root is depth 0 for every contract. When the flag is set the
   branch point is `depth + selfExtLen`, not `depth`, so both the nibble followed and the returned depth
   move. Derive `selfExtLen` from `layout()`, which bounds-checks (`:380-385`) — **never read `record[1]`
   directly**: a 1-byte record panics, and an unrecovered panic in a warm worker kills the process over
   best-effort data.
2. **`NewRecord` takes a plane-local depth.** For the storage plane the loop's `depth` is the global
   64..128 value while records are depth-local (`unfold.go:73,77` pass `len(path)`; `Validate:260` rejects
   `> 63`). Harmless for the three accessors above, latent for anything later calling `leafAt`. Pass
   `depth-64` on the storage plane.
3. **`l.ok == false` and "no child" both return `stop`**, indistinguishable from a successful terminal
   descent. Only the Task 4 counter tells them apart.
4. **`extAt` walks the trailer from `trailerStart()`** (`record.go:161-178`) on every step. Correct and
   bounded by 16; do not memoise it, and do not test it — `record_decode_test.go:229,276` already covers it.

### Plane crossing

`hashedKey` is 64 nibbles for an account, 128 for a slot (`phase_a.go:72` guarantees one or the other).

```
len(hashedKey) <= 64                 ->  AccountNodeKey(hashedKey[:depth], dst)
len(hashedKey) > 64 && depth <  64   ->  AccountNodeKey(hashedKey[:depth], dst)
len(hashedKey) > 64 && depth >= 64   ->  StorageNodeKey(pack(hashedKey[:64]), hashedKey[64:depth], dst)
```

**The gate is key length, not depth alone.** The loop bound is `depth <= len(hashedKey)`, so a 64-nibble
account key reaches `depth == 64`; keying on `depth >= 64` would send it to `StorageNodeKey` and warm a
storage root for every EOA.

The account descent terminates at the account leaf (~depth 7), so for a 128-nibble key the **step function**
returns 64 instead of `stop` at that point — the restart is a v4 rule and must not sit in the shared loop,
where it would also fire for HPH's 128-nibble storage keys and break the bit-identical requirement. The
existing LCP dedupe (`commitment.go:1876-1884`) already makes it fire once per contract: later slots of the
same contract arrive with `startDepth >= 64` and skip the account plane.

### Where warmup overlaps in v4, and where it does not

In HPH the fold *is* the `HashSort` callback, so warmup workers run ahead of the reader within one pass. In
v4, `HashSort`'s callback is only `p.add` (`trie.go:152`) — the partitioner — and every real record read
happens afterwards in `runScheduledPhases` (`:165`). So warmup overlaps the **partition** pass, not the
fold, and the arena ring barrier (`WaitBufferFree`) throttles partitioning to warmup's rate rather than
hiding read latency behind compute.

That may still be the right place — it prefetches strictly ahead of every phase read — but it is a different
shape from HPH and it is **unmeasured**. `CloseAndWait` must be deferred to the end of `Process`, not before
`runScheduledPhases`. Task 5 measures whether the overlap pays; do not assume it does.

### What stays untouched

The LCP dedupe, the arena ring barrier (`WaitBufferFree:230`; call sites `commitment.go:1784,1824,1855,1904`),
the worker pool, the `CtxFactory`, the epoch/coherence model, the put stripes, and every V1/V2 routing path.

## Progress Tracking

- mark completed items `[x]` immediately when done
- `➕` prefix for newly discovered tasks, `⚠️` for blockers
- keep this file in sync with the work actually done

## What Goes Where

- **Implementation Steps** — code, tests, and the measurement run, all achievable in this worktree.
- **Post-Completion** — the mainnet-bed measurement and the follow-up decisions it unblocks.

## Implementation Steps

### Task 1: Route v4 keys in BranchCache by tag

**Files:**
- Modify: `execution/commitment/branch_cache.go`
- Create: `execution/commitment/branch_cache_v4_test.go`

- [ ] capture the Task 5 baseline arm first, before any edit: per-tier `BranchCache.Stats()` (`:639`) and
      `warmuper.Stats()` on a v4 run at this HEAD — it is unrecoverable once Task 1 lands
- [x] write the failing tier table test in `branch_cache_v4_test.go`: for each v4 key shape (account root
      `40 00`, account depths 1-6, storage root `41||H||00`, storage depths 1-6) assert the entry lands in
      the intended tier and reads back byte-identical
- [x] write the `PinEntry` reachability test: pin a v4 account key, then `Get` it — today it lands in the
      tail while `lookup:442-450` stops at a trunk miss without falling through, so it is unreachable
- [x] write the collision test: generate distinct v4 keys across depths 0-6 in both planes, `Put` a
      distinct value in each, assert every `Get` returns its own value — a trunk slot stores no key, so a
      collision is a silently wrong record
- [x] write the dispatch-precondition assertion: no compact prefix produced by `nibbles.HexToCompact` can
      begin `0x40`/`0x41`/`0x42` (`nibbles.go:55-58` sets only `0x20`/`0x10`), alongside the assertion that
      `IsCommitmentStateKey` (`commitment.go:167`) already excludes `0x42`
- [x] confirm the tests are red for the right reason — name the assertion and the observed tier, not the
      exit code
- [x] add a tag discriminator in `branch_cache.go` dispatching `0x40`/`0x41` to the new routing and
      everything else to the existing compact path
- [x] route all three entry points through it: `lookup:431`, `store` (via `Put:563`) and `PinEntry:508`
- [x] extend `isRootPrefix` (`:427`) to accept `40 00` so the v4 account root reaches the `root` tier
- [x] mutation-verify each guard: copy `branch_cache.go` to the scratchpad, revert the whole dispatch,
      run the whole package, name the `file:line` that went red, copy the file back
- [x] run `go test ./execution/commitment/... ./db/state/...` — all green before Task 2

### Task 2: Lift key-building and record-stepping out of the warmupper

**Files:**
- Modify: `execution/commitment/warmuper.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/exec/bal_commitment_warmup.go`
- Modify: `execution/commitment/warmuper_test.go`
- Modify: `execution/commitment/testutil_test.go`

- [ ] add `WarmupKeyFunc` and `WarmupStepFunc` declarations and the two `WarmupConfig` fields in
      `warmuper.go:37`, beside the struct — not in `config.go`
- [ ] move the V2 key construction (`HexToCompactInto`, `warmuper.go:148`) and the V2 record parse
      (`:165-200`, including `skipCellFields` use) out of `warmupKey` into `hex_patricia_hashed.go` as the
      two functions HPH supplies, preserving the existing descent decisions exactly
- [ ] rewrite `warmupKey` (`:144`) as a format-agnostic loop over the two functions, holding only
      `startDepth`, `maxDepth` and the stop condition — no plane logic, no nibble selection
- [ ] have `hex_patricia_hashed.go:2622-2628` populate both fields when constructing the Warmuper
- [ ] update `execution/exec/bal_commitment_warmup.go:134` — an out-of-package `NewWarmuper` site that sets
      neither field and relies on the parse being moved — to supply the HPH pair
- [ ] update `testutil_test.go:190` and the six `warmuper_test.go` construction sites
- [ ] make a nil `Key` or `Step` **panic** in `NewWarmuper` (`:80` returns no error), or return it from
      `Start()` the way the nil-`PatriciaContext` case already does at `:113-118` — pick one and say which
- [ ] write a test that the HPH key function reproduces `HexToCompactInto` for depths 0-64, both parities
- [ ] write a test that the HPH step function reproduces the old descent decisions on a V2 record fixture
      (child present, child absent, leaf terminator, extension advance, truncated record)
- [ ] write a test that a nil `Key` or `Step` is rejected, not silently treated as no-descent
- [ ] mutation-verify the HPH step test by file copy: invert the extension advance, confirm red, restore
- [ ] run `go test ./execution/commitment/... ./execution/exec/...` — HPH behaviour unchanged before Task 3

### Task 3: Supply the v4 key and step functions and stop dropping the warmuper

**Files:**
- Create: `execution/commitment/v4/warmup.go`
- Create: `execution/commitment/v4/warmup_test.go`
- Modify: `execution/commitment/v4/trie.go`
- Modify: `execution/commitment/v4/key.go`
- Modify: `execution/commitment/v4/unfold.go`

- [ ] write the failing test first: a v4 fixture trie where a descent from depth 0 reaches the account leaf,
      asserting the exact sequence of keys the pair produces
- [ ] create `v4/warmup.go` (2026 license header) with the key function implementing the plane crossing
      gated on `len(hashedKey) > 64`, never on `depth` alone, writing into the caller's `dst`
- [ ] implement the step function per Technical Details: `tree()` as the descent predicate, plane-local
      depth into `NewRecord`, and the self-extension branch point derived from `layout()` — never from a
      direct `record[1]` read
- [ ] return `64` from the step function instead of `stop` when a 128-nibble key finishes the account plane,
      so the restart stays v4-local and never reaches HPH's 128-nibble keys
- [ ] in `v4/trie.go:132-160`, build the Warmuper behind an `if warmup.Enabled` gate mirroring
      `hex_patricia_hashed.go:2623` — without it every `WarmupConfig{}` in the v4 test files trips Task 2's
      nil check — and pass it to `updates.HashSort` in place of `nil`
- [ ] defer `CloseAndWait` to the end of `Process`, after `runScheduledPhases`, not before it
- [ ] give `nodeKey` (`key.go:99`) a pack scratch so `packPath(path, nil)` at `:105` stops allocating, and
      thread the buffer from `unfold.go:53,60` — both halves, or the key allocation survives
- [ ] write the plane-crossing test: a storage key produces account-plane keys below 64 and storage-plane
      keys at or above it; a 64-nibble account key reaching `depth == 64` produces **no** storage key; a
      second slot of the same contract with `startDepth >= 64` skips the account plane entirely
- [ ] write the self-extension test: a root record with `hdrHasSelfExt` descends to the correct child, with
      a sibling nibble asserted *not* followed, and a 1-byte record returns `stop` without panicking
- [ ] write the key scratch-reuse test: consecutive calls with a carried buffer match fresh allocations,
      including the odd-length path where the trailing half-byte must be zeroed, and assert zero allocs
      with `testing.AllocsPerRun`
- [ ] run `TestLegacyVsHexRoot` and `TestIncrementalRootsAgree` — the root must be identical with warmup on
      and off
- [ ] run `go test ./execution/commitment/...` — all green before Task 4

### Task 4: Count records found, so a dead descent cannot pass

**Files:**
- Modify: `execution/commitment/warmuper.go`
- Modify: `execution/commitment/v4/warmup_test.go`
- Modify: `execution/commitment/warmuper_test.go`

- [ ] write the failing test first: on a v4 fixture, `Stats().RecordsFound` must be greater than zero — it
      is zero today and stays zero under every one of the traps
- [ ] add `RecordsFound` to `WarmupStats` and the backing `atomic.Uint64`, incremented in `warmupKey` only
      when a read returns a non-empty record
- [ ] assign `w.startTime` in `Start()` — it is declared at `:64` and read at `:250-251` but never set, so
      `Stats().Duration` is always 0 and Task 5 would measure nothing
- [ ] write the trap-3 test: a truncated record (`layout().ok == false`) and a genuinely absent child both
      stop, and `RecordsFound` distinguishes them
- [ ] write a `Stats().Duration > 0` test
- [ ] mutation-verify the `RecordsFound` guard by file copy: revert the v4 key function to the V2 compact
      key, confirm the nonzero assertion goes red while every other test still passes, restore
- [ ] run `go test ./execution/commitment/...` — all green before Task 5

### Task 5: Measure, then decide the deferred items

**Files:**
- Modify: `docs/plans/20260922-commitment-v4-cache-and-warmup.md`
- Create: `~/org/mode/e/research/commitment-v4-cache-warmup.org`

- [ ] start a research log (`research-log` skill): the question, the numbers, and any claim that dies
- [ ] capture the after arm on the same corpus and machine as the Task 1 baseline, interleaved, not
      sequentially
- [ ] record per-tier hit rate, `RecordsFound`, `Duration`, `staleEvicted` and `bytesServed` with
      provenance — the machine, the block range, the arm
- [ ] measure the overlap question from Technical Details: does warming during the partition pass pay, or
      does the arena barrier just throttle partitioning? Compare `Process` wall clock with warmup on and off
- [ ] answer the leaf question from the numbers: what share of reads is the deepest branch per key, and is
      it shareable at all
- [ ] decide and record, with the number behind each: cold-start seed pass (yes/no), leaf warming (yes/no),
      removing the `branchBuf` copy on a cache hit (yes/no), lazy ext unpack (yes/no)
- [ ] write each rejected option into the log with the measurement that killed it, so it is not re-derived
- [ ] update this plan's "What this plan does not do" section with the outcomes

### Task 6: Verify acceptance criteria

- [ ] every v4 key shape reaches its intended cache tier through **all three** entry points — `lookup`,
      `Put` and `PinEntry` — proven by the Task 1 table and reachability tests
- [ ] no two distinct v4 keys share a trunk slot, proven by the Task 1 collision test
- [ ] the V1/V2 compact routing path is bit-identical: `git diff` shows no behavioural change to
      `trunkSlot`, `storageRoute`, `ContractHashFromPrefix` or `storageNibbles` for non-v4 keys
- [ ] `warmuper.go` contains no record-format knowledge — grep it for `fieldAccountAddr`, `skipCellFields`,
      `HexToCompact`; all three must be absent
- [ ] `warmupKey` contains no plane logic and no nibble selection
- [ ] `v4/trie.go` no longer passes `nil` to `HashSort`, and gates on `warmup.Enabled`
- [ ] a v4 node read allocates no key: `testing.AllocsPerRun` on the unfold path
- [ ] `Stats().RecordsFound > 0` on a v4 run, and `0` when the key function is reverted
- [ ] the state root is unchanged with warmup on and off: `TestLegacyVsHexRoot`, `TestIncrementalRootsAgree`
- [ ] run the full suites: `go test ./execution/commitment/... ./db/state/... ./execution/exec/...`
- [ ] run the full `execution/commitment` package under `-race` — unfiltered, because the warmup workers
      mutate shared counters and a filtered run drops the parallel arms
- [ ] run `go tool -modfile=golangci-lint.mod golangci-lint run ./execution/commitment/... ./db/state/...`

### Task 7: [Final] Update documentation

- [ ] update `execution/commitment/v4/README.md` with the cache routing rule and the warmup seam
- [ ] correct the v4 architecture doc's §8, which states the cache is "already shaped for this" — it is
      shaped for the compact key and was not — and add the two function types to §9's API seam list
- [ ] file the trunk preload in `docs/backlog/` (`preload_parallel.go:50`, `preload_ranges.go:18,25,26`
      still build compact keys under v4) — note the backlog directory lives in the main checkout, not a
      worktree
- [ ] state in the PR body what the numbers were and which deferred items the measurement killed
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

**Measurement that needs a real bed** (local numbers are directional only):

- per-tier `BranchCache` hit rate on a mainnet-sized commitment domain — this is architecture-doc **M6**,
  and it also sizes the §5(4) keccak cache. Bed: `snap-arb1` or `arb1-dev`. Log the host in `MACHINES.org`
  before the run.
- the warmup share of an incremental block, not the bulk arm — architecture-doc **M5**. Today's published
  fold numbers exclude the touch phase entirely.

**Decisions this unblocks** (architecture doc, §12 order):

- whether the cold-start seed pass earns its complexity, or the LCP dedupe plus a resident trunk already
  covers it after block 1
- whether the trunk depth (`trunkDepthFull = 4`) is still the right cut for v4's key shape
- whether the trunk preload is worth porting to v4 keys at all, or should be deleted with the rest of the
  compact-key surface
- step 3 of the architecture doc's order — v4 in shadow mode with the root compared every block — which is
  the next thing that should land after this plan
