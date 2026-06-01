# TrieConfig Slim — cut speculative surface, wire seams honestly (PR #20559)

## Overview

PR #20559 ("commitment: consolidate scattered config into TrieConfig struct") consolidated
scattered commitment-trie configuration (constructor params, post-construction setters, magic
numbers) into a single `commitment.TrieConfig`. A brainstorm found the struct overshot: several
fields are speculative — never set to a non-default by any production caller — and one dbg env
knob is wired ad-hoc at a non-obvious site.

This plan slims `TrieConfig` to its essential, reviewable core so the long-open PR merges cleanly.
It keeps 100% of the consolidation value (named constants, single struct, single config home) while
removing the unused per-instance machinery and the dead code paths that invite "why is this here /
is this tested?" review questions.

Three independent threads:

1. **Cut dead numeric knobs** — `MaxDeferredUpdates` and `RebuildShardMaxSteps` fields + their
   `*OrDefault()` accessors. Never configured per-instance; the two read sites use the exported
   `Default*` consts directly.
2. **dbg knob, one honest home** — the `dbg.TipTrieWarmupers` default is currently resolved across
   *two* scattered sites (a construction-time override and a use-site fallback), while the
   `WarmupNumWorkersOrDefault()` accessor sits unused. Fold the env logic into that accessor, route
   the live path through it, and delete both scattered sites. Behavior-preserving; collapses two
   sites into one obvious home.
3. **`Subtrie()` derivation, drop the field** — replace `SpawnSubTrie`'s implicit
   derive-from-parent + inline mutation with a named `TrieConfig.Subtrie()` method; remove the
   never-used `SubtrieConfig *TrieConfig` field and its dead nil-check branch.

## Context (from discovery)

- **Files involved:**
  - `execution/commitment/config.go` — the `TrieConfig` struct, `Default*` consts, `*OrDefault()` accessors
  - `execution/commitment/hex_patricia_hashed.go` — `applyConfig` (read site `:163`), `SpawnSubTrie` (`~:126-142`)
  - `execution/commitment/commitmentdb/commitment_context.go` — ad-hoc dbg override (`:188-191`)
  - `db/state/squeeze.go` — `RebuildShardMaxStepsOrDefault()` read site (`:888`)
  - `execution/commitment/config_test.go` — tests for accessors + subtrie inheritance
  - `common/dbg/experiments.go` — `TipTrieWarmupers` (`TIP_TRIE_WARMUPERS`, default `runtime.NumCPU()*8`)
- **Patterns found:** the commitment package already imports `common/dbg` (commitment.go,
  hex_patricia_hashed.go, metrics.go), so config.go importing it is cycle-free (`common/dbg` does
  not import commitment).
- **Load-bearing fields kept:** `Variant`, `DeferBranchUpdates`, `LeaveDeferredForCaller`,
  `EnableWarmupCache`, `EnableTrieWarmup`, `CsvMetricsFilePrefix`, `MemoizationOff`, `WarmupNumWorkers`.
- **Branch state:** `awskii/commitment-config`, freshly merged with `main` (clean build). This plan
  is self-contained from a clean git state.

## Development Approach

- **Testing approach: Regular, refactor exemption.** This is a pure refactor with no behavior
  change. Per the repo `CLAUDE.md` "When pragmatism applies" section, pure refactors are exempt from
  the Red→Green→Refactor cycle — the **existing test suite is the safety net**. Do not invent new
  behavioral tests; instead keep the existing tests compiling and green, and rewrite the tests that
  reference removed symbols so they exercise the new equivalent (e.g. `Subtrie()` instead of
  `SpawnSubTrie` round-trip).
- Complete each task fully before the next. Each task is independently buildable.
- **Every task must end green:** `make lint` clean (linter is non-deterministic — run repeatedly
  until clean) **and** `go build ./...` plus test-compile of touched packages pass.
- **Never add `t.Skip`** (or any test-muting form) — forbidden for automated agents per repo
  `CLAUDE.md`. If a test fails, fix the underlying cause or escalate.
- **Do NOT push.** Pushing PR #20559 is gated on explicit user approval after `make lint`.
- Commit-message prefix = package(s) modified, e.g. `commitment: drop unused TrieConfig numeric fields`.

## Testing Strategy

- **Unit tests:** the affected unit tests live in `execution/commitment/config_test.go` and the
  trie/squeeze test suites. Tasks update these to track symbol removals/renames and must keep them
  passing. No new behavioral tests are required (pure refactor).
- **Compile gate per task:**
  `go build ./...` and
  `go test -count=0 ./execution/commitment/... ./db/state/... ./rpc/jsonrpc/receipts/...`
  (test-compile only) must succeed.
- **e2e tests:** none apply (no UI).

## Progress Tracking

- mark completed items `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document blockers with ⚠️ prefix
- update this plan if scope changes during implementation

## Solution Overview

The struct loses 3 fields + 2 accessors + 1 dead branch and gains 1 small method. Every remaining
field is provably set by some caller or runtime path. The dbg→config wiring moves from a buried
constructor block to the accessor every caller already invokes, so behavior is preserved while the
"single config home" story becomes true. Subtrie derivation becomes an explicit, named, once-declared
rule instead of an implicit derive-and-mutate with a dead alternate branch.

## Technical Details

- **Thread 1 read-site replacements** (preserve existing types):
  - `hex_patricia_hashed.go:163`: `cfg.maxDeferredUpdatesOrDefault()` → `DefaultMaxDeferredUpdates`
    (assigned to `branchEncoder.maxDeferredUpdates int`; the untyped const fits).
  - `squeeze.go:888`: `rebuildTrieCfg.RebuildShardMaxStepsOrDefault()` →
    `uint64(commitment.DefaultRebuildShardMaxSteps)` (keep `maxShardSteps` a `uint64`). Verify no
    struct literal in squeeze.go still sets the now-removed `RebuildShardMaxSteps` field.
- **Thread 2 — the real value chain (corrected after plan review).** `WarmupNumWorkersOrDefault()`
  has **zero production callers**. The live chain is:
  `commitment_context.go:190-191` (env override mutates `cfg.WarmupNumWorkers`) →
  `:198` (`warmupBase.NumWorkers = cfg.WarmupNumWorkers`, **raw**) →
  `:419-420` (use-site fallback: `if warmupConfig.NumWorkers == 0 { = DefaultWarmupNumWorkers }`).
  For an unset config today the result is `dbg.TipTrieWarmupers` (default `NumCPU*8`); the `16`
  fallback only fires if `TIP_TRIE_WARMUPERS<=0`. Simply moving the env logic into the dead accessor
  would **drop the live path to 16** — a real regression. The correct fix makes the accessor the
  single home AND routes the live path through it, which lets us delete BOTH scattered sites:
  - accessor body (config.go):
    ```go
    func (c TrieConfig) WarmupNumWorkersOrDefault() int {
        if c.WarmupNumWorkers != 0 {
            return c.WarmupNumWorkers
        }
        if dbg.TipTrieWarmupers > 0 {
            return dbg.TipTrieWarmupers
        }
        return DefaultWarmupNumWorkers
    }
    ```
  - `commitment_context.go:198`: change `NumWorkers: cfg.WarmupNumWorkers` → `NumWorkers: cfg.WarmupNumWorkersOrDefault()`.
  - delete the `:188-191` override block (the `if cfg.WarmupNumWorkers == 0 && dbg.TipTrieWarmupers > 0 {...}` lines + comment).
  - delete the `:419-420` use-site fallback (`if warmupConfig.NumWorkers == 0 { ... }`) — now dead, since `warmupBase.NumWorkers` is resolved-nonzero at construction.
  - update the `warmupBase` doc comment at `:53-55` (it currently says "NumWorkers is stored raw … defaulted at the use site"; after the change it stores the resolved value).
  - `commitment_context.go` keeps its `dbg` import (still used by `TrieTrace*` at `:346-350`).
  - **Equivalence to verify:** unset config → `NumCPU*8` (was: env override; now: accessor), and `TIP_TRIE_WARMUPERS<=0` → `16` (was: use-site fallback; now: accessor). Identical outcomes, two sites → one.
- **Thread 3 method + call site:**
  ```go
  // config.go
  func (c TrieConfig) Subtrie() TrieConfig {
      s := c
      s.DeferBranchUpdates = false
      return s
  }
  ```
  `SpawnSubTrie` (hex_patricia_hashed.go ~:126-142): replace the
  `var subCfg TrieConfig; if hph.cfg.SubtrieConfig != nil { subCfg = *hph.cfg.SubtrieConfig } else { subCfg = hph.cfg }; subCfg.DeferBranchUpdates = false; subCfg.SubtrieConfig = nil` block with
  `subCfg := hph.cfg.Subtrie()`. Remove the `SubtrieConfig *TrieConfig` struct field and its doc line.

## What Goes Where

- **Implementation Steps** (`[ ]`): all code + test changes — achievable in this repo.
- **Post-Completion** (no checkboxes): `make lint` final pass note and the gated manual push of PR #20559.

## Implementation Steps

### Task 1: Cut dead numeric knobs (MaxDeferredUpdates, RebuildShardMaxSteps)

**Files:**
- Modify: `execution/commitment/config.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `db/state/squeeze.go`
- Modify: `execution/commitment/config_test.go`

- [ ] in `config.go`, remove the `MaxDeferredUpdates int` and `RebuildShardMaxSteps uint64` fields from the `TrieConfig` struct (and their doc comments); keep the exported `DefaultMaxDeferredUpdates` and `DefaultRebuildShardMaxSteps` consts
- [ ] in `config.go`, delete the `maxDeferredUpdatesOrDefault()` and `RebuildShardMaxStepsOrDefault()` methods
- [ ] in `hex_patricia_hashed.go:163`, replace `cfg.maxDeferredUpdatesOrDefault()` with `DefaultMaxDeferredUpdates`
- [ ] in `db/state/squeeze.go:888`, replace `rebuildTrieCfg.RebuildShardMaxStepsOrDefault()` with `uint64(commitment.DefaultRebuildShardMaxSteps)` (the `commitment` import is already present at `squeeze.go:36`); `squeeze.go:887` uses `DefaultTrieConfig()` with no field override, so nothing else needs touching
- [ ] `config_test.go` `TestDefaultTrieConfig`: delete the `cfg.MaxDeferredUpdates` block (lines ~21-23) and the `cfg.RebuildShardMaxSteps` block (lines ~39-41)
- [ ] `config_test.go` `TestTrieConfig_OrDefaultHelpers` (~47-63): remove the two `RebuildShardMaxStepsOrDefault()` assertions and drop `RebuildShardMaxSteps: 7` from the `TrieConfig{...}` literal (leave the `WarmupNumWorkers` assertions — they are still valid until Task 3)
- [ ] `config_test.go`: delete `TestTrieConfig_maxDeferredUpdatesOrDefault` (~65-78) entirely (tests a removed accessor)
- [ ] `config_test.go`: delete `TestTrieConfig_MaxDeferredUpdatesApplied` (~173-193) entirely (it sets the removed `MaxDeferredUpdates` field; the const is now hard-applied in `applyConfig`, not configurable)
- [ ] run `go build ./...` + `go test -count=0 ./execution/commitment/... ./db/state/...`; then `make lint` until clean — must pass before Task 2

### Task 2: Add Subtrie() derivation method, drop SubtrieConfig field

**Files:**
- Modify: `execution/commitment/config.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/config_test.go`

- [ ] in `config.go`, add `func (c TrieConfig) Subtrie() TrieConfig { s := c; s.DeferBranchUpdates = false; return s }`
- [ ] in `config.go`, remove the `SubtrieConfig *TrieConfig` field and its doc line from the `TrieConfig` struct
- [ ] in `hex_patricia_hashed.go` `SpawnSubTrie` (~:126-142), replace the `var subCfg TrieConfig; if hph.cfg.SubtrieConfig != nil {...} else {...}; subCfg.DeferBranchUpdates = false; subCfg.SubtrieConfig = nil` block with `subCfg := hph.cfg.Subtrie()` (drop the now-obsolete "force-disable" comment)
- [ ] `config_test.go` `TestDefaultTrieConfig`: delete the `cfg.SubtrieConfig != nil` block (lines ~36-38) — the field no longer exists
- [ ] `config_test.go`: add a focused unit test `TestTrieConfig_Subtrie` asserting `cfg.Subtrie()` sets `DeferBranchUpdates == false` and copies every other field unchanged (e.g. `LeaveDeferredForCaller`, `MemoizationOff`, `Variant`)
- [ ] keep `TestTrieConfig_SpawnSubTrieInheritsConfig` (~108-134) and `TestTrieConfig_ConcurrentPatriciaHashedPropagation` (~136-171) as-is — they reference no removed symbol and still validate the `SpawnSubTrie`/concurrent consumer end-to-end
- [ ] run `go build ./...` + `go test -count=0 ./execution/commitment/...`; then `make lint` until clean — must pass before Task 3

### Task 3: Move dbg.TipTrieWarmupers into WarmupNumWorkersOrDefault (one home)

**Files:**
- Modify: `execution/commitment/config.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `execution/commitment/config_test.go`

- [ ] in `config.go`, add `import "github.com/erigontech/erigon/common/dbg"` (config.go does not yet import it; the package overall already depends on dbg, so no cycle)
- [ ] in `config.go`, rewrite `WarmupNumWorkersOrDefault()` to the corrected body in Technical Details: field if nonzero → else `dbg.TipTrieWarmupers` if `>0` → else `DefaultWarmupNumWorkers`
- [ ] in `commitment_context.go:198`, change `NumWorkers: cfg.WarmupNumWorkers` to `NumWorkers: cfg.WarmupNumWorkersOrDefault()`
- [ ] in `commitment_context.go`, delete the override block at `:188-191` (the `if cfg.WarmupNumWorkers == 0 && dbg.TipTrieWarmupers > 0 { ... }` lines and its comment)
- [ ] in `commitment_context.go`, delete the now-dead use-site fallback at `:419-420` (`if warmupConfig.NumWorkers == 0 { warmupConfig.NumWorkers = commitment.DefaultWarmupNumWorkers }`)
- [ ] in `commitment_context.go`, update the `warmupBase` doc comment (~:53-55) — it no longer "stores raw … defaulted at the use site"; it stores the resolved worker count
- [ ] confirm `commitment_context.go` still uses its `dbg` import (`TrieTrace*` at `:346-350`); leave those untouched
- [ ] `config_test.go` `TestTrieConfig_OrDefaultHelpers`: update the zero-config `WarmupNumWorkersOrDefault()` assertion — expected is now `dbg.TipTrieWarmupers` when `>0` else `DefaultWarmupNumWorkers` (compute the expected from `dbg.TipTrieWarmupers` so it stays deterministic regardless of host `NumCPU`); the explicit `WarmupNumWorkers: 3 → 3` case is unchanged
- [ ] verify equivalence (see Technical Details): unset config resolves to `NumCPU*8`, `TIP_TRIE_WARMUPERS<=0` resolves to `16` — identical to pre-change outcomes
- [ ] run `go build ./...` + `go test -count=0 ./execution/commitment/... ./db/state/... ./rpc/jsonrpc/receipts/...`; then `make lint` until clean — must pass before Task 4

### Task 4: Verify acceptance criteria

- [ ] `TrieConfig` struct contains only load-bearing fields: `Variant`, `DeferBranchUpdates`, `LeaveDeferredForCaller`, `EnableWarmupCache`, `EnableTrieWarmup`, `CsvMetricsFilePrefix`, `MemoizationOff`, `WarmupNumWorkers` (no `MaxDeferredUpdates`, `RebuildShardMaxSteps`, `SubtrieConfig`)
- [ ] `grep -rn "SubtrieConfig\|maxDeferredUpdatesOrDefault\|RebuildShardMaxStepsOrDefault" --include='*.go'` returns no matches outside this plan/docs
- [ ] no `t.Skip` (or any mute form) was added anywhere
- [ ] full verification: `make lint` (until clean) then `make erigon integration` both succeed
- [ ] run the commitment + state test suites (not just compile): `go test ./execution/commitment/... ./db/state/...`

### Task 5: [Final] Update plan tracking

- [ ] confirm all checkboxes above are `[x]`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Informational — requires explicit user action; not part of automated execution.*

**Gated manual push (user-approved only):**
- After `make lint` is clean, the user pushes `awskii/commitment-config` to update PR #20559.
  The plan executor must NOT run `git push` — pushing this PR is a deliberately manual, gated step.

**Reviewer-facing note for the PR description:**
- This is a pure refactor: removes never-configured `TrieConfig` fields/accessors, centralizes the
  `TIP_TRIE_WARMUPERS` fallback in `WarmupNumWorkersOrDefault()`, and replaces implicit subtrie
  derivation with a named `Subtrie()` method. No behavior change.
