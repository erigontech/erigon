# SharedDomains functional-options constructor

## Overview

Address taratorio's review on PR #20559: replace `NewSharedDomainsWithTrieConfig(ctx, tx, logger, cfg)` — whose `WithBlahBlah` name does not scale (`WithTrieConfigAndWithSomethingElseConfig`) — with the Go functional-options pattern. Collapse the two constructors (`NewSharedDomains` + `NewSharedDomainsWithTrieConfig`) into one variadic `NewSharedDomains(ctx, tx, logger, opts ...SharedDomainOption)`.

The `commitment.TrieConfig` consolidation this PR already shipped stays as-is; this is the final refactor on top so the PR can merge.

Side effect (intended): the 13 read-only/one-shot call sites stop passing a zero-value `TrieConfig{Variant: ...}` (which silently disabled warmup) and instead use `WithoutDeferredBranchUpdates()`, which starts from `DefaultTrieConfig()` (warmup ON) and only flips defer off — restoring the intended `DefaultTrieConfig` warmup semantics that the PR's own zero-value path regressed earlier. (Note: `TrieConfig`/`DefaultTrieConfig()` are introduced *by this PR*; `origin/main` had no `TrieConfig` at all — it passed only a `TrieVariant` and warmup was effectively on. So the fix is measured against intended defaults, not a literal source diff with main.)

## Context (from discovery)

- **Constructors**: `db/state/execctx/domain_shared.go:177` (`NewSharedDomains` wrapper) and `:183` (`NewSharedDomainsWithTrieConfig`). `PickTrieVariant()` at `:170`.
- **Config struct**: `commitment.TrieConfig` + `DefaultTrieConfig()` in `execution/commitment/config.go` (already consolidated, reviewed).
- **17 external call sites** use `NewSharedDomainsWithTrieConfig` (no `_test.go` among them — confirmed via grep). ~121 plain `NewSharedDomains(ctx, tx, logger)` callers exist and are untouched by a variadic signature (no caller passes a 4th positional arg).
- **`SetDeferBranchUpdates` is already fully removed** from the codebase (grep returns nothing) — this plan does not need to reintroduce or migrate it.
- Two call-site shapes:
  - **13 sites**: `commitment.TrieConfig{Variant: execctx.PickTrieVariant()}` (zero-value → defer off, warmup off).
  - **4 sites**: a custom full `DefaultTrieConfig()` with 1–2 fields tweaked + an explicit variant (squeeze ×3, backtester ×1).

## Development Approach

- **Testing approach**: Regular. This is a **pure refactor with no behavior change vs `origin/main`** — TDD does not apply (per repo CLAUDE.md "pragmatism applies" exception for pure refactors; existing tests are the safety net). State this explicitly in the PR/commit. No new tests are required; do not invent tests that merely echo the new code.
- Complete each task fully before the next; keep changes small and focused.
- **Verification gate after every code task**: the package must compile. Full gate (`make lint && make erigon integration`) runs in the verification task.
- Plan must be self-contained from a clean git state (may run via ralphex). It does not depend on any in-progress working-tree state.

## Testing Strategy

- **Unit tests**: none added — pure refactor, no behavior change. The existing `db/state` / commitment / rpc test suites are the regression net and must stay green.
- **No e2e tests** in scope (no UI).
- Regression check is the build + lint + existing suite, performed in the Verify task.

## Progress Tracking

- Mark completed items `[x]` immediately when done.
- `➕` prefix for newly discovered tasks; `⚠️` prefix for blockers.
- Update this plan if scope changes.

## Solution Overview

`NewSharedDomains` becomes the single variadic constructor. The base trie config is built **inside** the constructor (`DefaultTrieConfig()` + `Variant = PickTrieVariant()`), so `Variant: PickTrieVariant()` stops being copy-pasted across call sites. Options mutate a small internal `sharedDomainOptions` carrier.

Exactly **two** options, kept together in a new `options.go` — straight options, not a setter per struct field:

- `WithTrieConfig(cfg)` — full-config escape hatch (replaces the whole config; caller owns `Variant`). Used by squeeze + backtester, which build bespoke configs.
- `WithoutDeferredBranchUpdates()` — the one sanctioned single-field toggle. Used by the 13 read-only/one-shot sites.

No magic zero-value variant fallback: when `WithTrieConfig` is used, the caller's `Variant` is authoritative (a reviewer already objected to zero-value fallbacks earlier in this PR).

## Technical Details

New file `db/state/execctx/options.go`:

```go
package execctx

import "github.com/erigontech/erigon/execution/commitment"

type sharedDomainOptions struct {
	trieCfg commitment.TrieConfig
}

// SharedDomainOption configures NewSharedDomains.
type SharedDomainOption func(*sharedDomainOptions)

// WithTrieConfig replaces the trie configuration wholesale; the caller owns Variant.
func WithTrieConfig(cfg commitment.TrieConfig) SharedDomainOption {
	return func(o *sharedDomainOptions) { o.trieCfg = cfg }
}

// WithoutDeferredBranchUpdates disables deferred branch updates (read-only / one-shot domains).
func WithoutDeferredBranchUpdates() SharedDomainOption {
	return func(o *sharedDomainOptions) { o.trieCfg.DeferBranchUpdates = false }
}
```

Constructor in `domain_shared.go` (replaces both current functions):

```go
func NewSharedDomains(ctx context.Context, tx kv.TemporalTx, logger log.Logger, opts ...SharedDomainOption) (*SharedDomains, error) {
	o := sharedDomainOptions{trieCfg: commitment.DefaultTrieConfig()}
	o.trieCfg.Variant = PickTrieVariant()
	for _, opt := range opts {
		opt(&o)
	}
	// ... existing body, passing o.trieCfg to commitmentdb.NewSharedDomainsCommitmentContext
}
```

Option ordering note: `WithTrieConfig` replaces the entire config including any prior `WithoutDeferredBranchUpdates`. In practice no call site combines them; document via the one-line godoc, no extra guard.

## What Goes Where

- **Implementation Steps** (checkboxes): new file, constructor merge, 17 call-site migrations, build/lint verification — all in this repo.
- **Post-Completion** (no checkboxes): update the PR #20559 body (external, GitHub).

## Implementation Steps

### Task 1: Add options.go with the two SharedDomainOption constructors

**Files:**
- Create: `db/state/execctx/options.go`

- [x] Create `db/state/execctx/options.go` with `sharedDomainOptions` struct, `SharedDomainOption` type, `WithTrieConfig`, and `WithoutDeferredBranchUpdates` (one-line godoc each, per repo no-comment-bloat rule).
- [x] `go build ./db/state/execctx/...` compiles (functions unused at this point is fine — they're exported).

### Task 2: Collapse the two constructors into one variadic NewSharedDomains

**Files:**
- Modify: `db/state/execctx/domain_shared.go`

- [x] Replace the wrapper `NewSharedDomains` (lines ~177–181) and `NewSharedDomainsWithTrieConfig` (lines ~183–211) with a single variadic `NewSharedDomains(ctx, tx, logger, opts ...SharedDomainOption)`.
- [x] Build base config inside: `o := sharedDomainOptions{trieCfg: commitment.DefaultTrieConfig()}`, `o.trieCfg.Variant = PickTrieVariant()`, apply opts, pass `o.trieCfg` to `commitmentdb.NewSharedDomainsCommitmentContext`.
- [x] Confirm no caller passes a 4th positional arg to `NewSharedDomains` (variadic is additive): plain callers compile unchanged; only the 17 `NewSharedDomainsWithTrieConfig` sites fail (migrated in Tasks 3/4).
- [x] `go build ./db/state/execctx/...` compiles; `NewSharedDomainsWithTrieConfig` no longer exists.

### Task 3: Migrate the 13 read-only/one-shot sites to WithoutDeferredBranchUpdates()

**Files:**
- Modify: `execution/engineapi/testing_api.go` (:87)
- Modify: `execution/builder/exec.go` (:132)
- Modify: `execution/builder/builder.go` (:149)
- Modify: `db/integrity/commitment_integrity.go` (:1066, :1134)
- Modify: `rpc/rpchelper/commitment.go` (:98)
- Modify: `rpc/jsonrpc/eth_call.go` (:464, :714)
- Modify: `rpc/jsonrpc/eth_simulation.go` (:167)
- Modify: `rpc/jsonrpc/debug_execution_witness.go` (:642, :1157)
- Modify: `rpc/jsonrpc/receipts/receipts_generator.go` (:328, :546)

- [x] Replace each `execctx.NewSharedDomainsWithTrieConfig(ctx, tx, logger, commitment.TrieConfig{Variant: execctx.PickTrieVariant()})` with `execctx.NewSharedDomains(ctx, tx, logger, execctx.WithoutDeferredBranchUpdates())`.
- [x] Import cleanup — Go treats an unused import as a compile error, so this is mandatory and explicit (do NOT guess):
  - **Remove** the `commitment` import (it becomes fully unused) in: `testing_api.go`, `exec.go`, `builder.go`, `rpc/rpchelper/commitment.go`, `eth_call.go`, `eth_simulation.go`, `receipts_generator.go`.
  - **KEEP** the `commitment` import in: `debug_execution_witness.go` (still used at ~:987 `commitment.NibblesToString`) and `commitment_integrity.go` (multiple other `commitment.` uses remain).
  - `execctx` stays imported everywhere (still used by the `NewSharedDomains` / `WithoutDeferredBranchUpdates` call) — `PickTrieVariant` is a member access, not a separate import; no `execctx` import removal anywhere.
- [x] `go build ./...` for the touched packages compiles (db/state fails only on the Task 4 squeeze.go sites, fixed next task).

### Task 4: Migrate the 4 custom-config sites to WithTrieConfig(cfg)

**Files:**
- Modify: `db/state/squeeze.go` (:483 rebuildCfg, :590 flushCfg, :994 iterTrieCfg)
- Modify: `execution/commitment/backtester/backtester.go` (:205 cfg)

- [x] Wrap each existing config in `execctx.WithTrieConfig(...)`: `execctx.NewSharedDomains(ctx, tx, logger, execctx.WithTrieConfig(rebuildCfg))` etc. Leave the cfg-building lines (including their explicit `Variant`) intact — `WithTrieConfig` replaces the whole config so their variant is honored.
- [x] `go build ./db/state/... ./execution/commitment/backtester/...` compiles.

### Task 5: Verify acceptance criteria

- [ ] `grep -rn "NewSharedDomainsWithTrieConfig" --include=*.go` returns nothing.
- [ ] `cd /Users/awskii/org/wrk/erigon-20559 && make erigon integration` builds clean.
- [ ] `make lint` is clean — run repeatedly until stable (linter is non-deterministic).
- [ ] Run the existing regression suites for touched areas: `go test ./db/state/... ./execution/commitment/... ./rpc/jsonrpc/...` (or `make test-short`) — all green, confirming no behavior change.
- [ ] Confirm the 13 sites end up with defer OFF, warmup ON — by inspection of `DefaultTrieConfig()` flag flow through `WithoutDeferredBranchUpdates()` (`DefaultTrieConfig` sets warmup flags true; the option flips only `DeferBranchUpdates`). This restores the intended default warmup semantics the PR's zero-value path had regressed; it is not a literal source diff against `origin/main` (which has no `TrieConfig`).

### Task 6: [Final] Documentation and plan close-out

- [ ] No README/CLAUDE.md change expected; confirm and skip if so.
- [ ] Move this plan to `docs/plans/completed/`.

## Post-Completion

*Items requiring external action — informational only, no checkboxes.*

**Update PR #20559 body (GitHub):**
- Describe the functional-options refactor: single variadic `NewSharedDomains` + `WithTrieConfig` / `WithoutDeferredBranchUpdates` in `db/state/execctx/options.go`; `NewSharedDomainsWithTrieConfig` removed.
- Note the warmup-consistency fix: read-only/one-shot domains keep warmup ON (matches `origin/main`); the zero-value `TrieConfig{Variant:...}` path that disabled it is gone.
- The current PR-body narrative around `SetDeferBranchUpdates` / `NonDeferredTrieConfig` is stale — replace it.
- This is a pure refactor with no behavior change vs `main`; state that (and that TDD therefore does not apply) per repo TDD-pragmatism guidance.
