# zkEVM Execution-Witness Test Suite (zkevm@v0.4.0)

## Overview

Add a new test suite that validates Erigon's `debug_executionWitness` RPC endpoint
against the `ethereum/execution-spec-tests` **zkevm@v0.4.0** fixtures. The suite
imports each fixture's blocks, calls `debug_executionWitness` per block, and
compares the returned witness (`state`, `codes`, `headers`) against the expected
values embedded in the fixture.

The corpus is included via the **PR #21002 fixture-manifest machinery** already on
`main` (`test-fixtures.json` + `tools/test-fixtures.sh`), not git submodules/LFS.

**Problem it solves:** Erigon currently has no automated regression coverage that
its execution-witness output matches the canonical EEST stateless-witness fixtures.
This suite makes witness correctness a first-class, CI-gated signal.

**Integration:** reuses the existing `execution/tests` block-test framework
(`BlockTest`, `TestMatcher`, `execmoduletester`) and the existing fixture-download
+ CI-shard infrastructure. The only new shared-code change is a small accessor that
exposes the test's `ExecModuleTester` for RPC-level assertions.

## Context (from discovery)

- **Repo:** `/Users/awskii/org/wrk/erigon`, origin `git@github.com:erigontech/erigon.git`. Go execution client. Branch convention `awskii/<desc>`.
- **Fixture machinery (already on `main`):**
  - `test-fixtures.json` maps `KEY -> {url, sha256, size}`.
  - `tools/test-fixtures.sh MANIFEST CACHE_DIR KEY...` downloads + sha256-verifies + extracts each key into `test-fixtures-cache/KEY/` (preserving the tarball's internal layout, e.g. `fixtures/blockchain_tests/...`). Re-runs are a no-op via a `.sha256` sentinel. **CI caches only `*.tar.gz` and re-extracts each run.**
  - `Makefile` has `test-fixtures-eest` / `test-fixtures-cl` targets.
  - `tools/test-groups` defines CI shards; workflows load names via `tools/test-groups names`.
- **`debug_executionWitness`** — `rpc/jsonrpc/debug_execution_witness.go`:
  - `func (api *DebugAPIImpl) ExecutionWitness(ctx, rpc.BlockNumberOrHash) (*ExecutionWitnessResult, error)` (line 524).
  - `ExecutionWitnessResult` (line 495): `State []hexutil.Bytes` json:`state`; `Codes []hexutil.Bytes` json:`codes`; `Keys []hexutil.Bytes` json:`keys` (always null); `Headers []map[string]any` json:`headers,omitempty` (Geth-style JSON objects via `marshalWitnessHeader`/`ethapi.RPCMarshalHeader`).
  - Requires commitment history: errors unless `rawdb.ReadDBCommitmentHistoryEnabled(tx)` is true.
  - Built via `jsonrpc.NewBaseApi(...)` + `jsonrpc.NewPrivateDebugAPI(base, db, ethBackend, gascap, gethCompatibility)`.
- **Block-test framework** — `execution/tests/testutil/`:
  - `BlockTest` (`block_test_util.go:53`); `func (bt *BlockTest) Run(t) error` (line 214) builds `m := execmoduletester.New(t, opts...)` (`WithGenesisSpec`, `WithEngine`, `WithExperimentalBAL`), inserts blocks, validates post-state + imported headers. **`Run` returns only `error` and does not expose the tester.**
  - `ExecModuleTester` (`execution/execmodule/execmoduletester/exec_module_tester.go:100`) exposes `DB kv.TemporalRwDB`, `Dirs`, `Engine`, `BlockReader`, `Genesis`, `Ctx`. Lifetime bound to `t` via `t.Cleanup` inside `execmoduletester.New` — callers MUST NOT `Close` it.
  - `TestMatcher` (`matcher.go`): `Walk(t, dir, runTestFn)`, plus `Fails`/`SkipLoad`/`CheckFailure` (**not used — see constraints**).
  - `testutil.RunTestMain(m)` for `TestMain`.
  - `statecfg.EnableHistoricalCommitment()` toggles the historical-commitment schema; `rawdb.WriteDBCommitmentHistoryEnabled(tx, true)` sets the DB flag the endpoint checks.
- **v0.4.0 release asset (confirmed via GitHub API):**
  - url `https://github.com/ethereum/execution-spec-tests/releases/download/zkevm%40v0.4.0/fixtures_zkevm.tar.gz`
  - size `221172523`
  - sha256 `f1d6dbec741e325a08d1227c2292be1aa228a78003d6d62b4ab5b53aefc8480a`
  - Based on bal-devnet-7 (Amsterdam).

## Development Approach

- **Testing approach:** Outside-in / TDD-flavored. The deliverable *is* a test suite, so the "tests" are the suite code; the `RunWithTester` seam (Task 4) is driven by how the runner (Task 6) needs to call it. Verification per task = the package builds and `make lint` is clean; the corpus run is the final acceptance gate.
- Complete each task fully before the next; small, focused changes.
- **HARD CONSTRAINT — no failure-muting (erigon CLAUDE.md):** automated agents must NEVER add `t.Skip`/`SkipNow`/`Skipf`, `bt.Fails(...)`, `SkipLoad`, build-tag exclusions, or env-gated bypasses. The suite asserts real pass/fail. Fixtures that fail (e.g. unimplemented Amsterdam EIPs 7708/7778/7843/7928) are surfaced to the user for triage / tracking issues — never silenced. If triage decides a documented, issue-linked skip is warranted, that is a **human** decision made after this plan, not part of agent implementation.
- **HARD CONSTRAINT — clean room:** do NOT use, copy, or reference worktree `erigon-20938` / branch `awskii/eest-witness-newmain`. It is considered faulty. All schema/comparison decisions come from inspecting real v0.4.0 fixture data and current `main`.
- Run `make lint` after changes (non-deterministic — run until clean) and before any push.

## Testing Strategy

- **Unit/integration tests:** the suite itself (`execution/tests/eest_zkevm_witness/`). The `RunWithTester` seam is exercised by `Run` (existing tests) and the new runner.
- **Schema-first:** the parser and comparison logic are written only after inspecting a real fixture (Task 2), not from assumptions.
- **No e2e/UI:** N/A for this codebase area.
- **Acceptance gate:** `make lint && make erigon integration`, then `make test-fixtures-zkevm && go test ./execution/tests/eest_zkevm_witness/...`; record real pass/fail counts.

## Progress Tracking

- mark completed items `[x]` immediately when done
- ➕ prefix for newly discovered tasks
- ⚠️ prefix for blockers (e.g. fixtures that fail due to missing EIP support — record fixture name + reason for user triage)
- keep this file in sync with actual work

## Solution Overview

Three layers:

1. **Fixture inclusion** — additive manifest entry + Makefile target + CI-shard registration, reusing PR #21002 machinery verbatim.
2. **Test seam** — minimal `BlockTest.RunWithTester` accessor returning the already-built `ExecModuleTester` so the runner can construct a `DebugAPIImpl` against the in-memory test DB. `Run` becomes a thin wrapper (zero behavior change for existing callers).
3. **Witness runner** — `WitnessBlockTest` parses the per-block `executionWitness`; the runner imports blocks, enables commitment history, calls `ExecutionWitness`, and compares arrays with real assertions and informative diffs.

Key design decisions:
- **Header comparison:** the RPC returns `[]map[string]any` whose maps **already embed a `"hash"` field** (from `ethapi.RPCMarshalHeader`), while the fixture stores headers as RLP-hex strings. Compare by reading the RPC map's `hash` directly and comparing it to the fixture header's computed hash (decode RLP-hex → `types.Header` → `.Hash()`). Decode only the fixture side; do NOT reconstruct a `types.Header` from the RPC map (that interface-soup reconstruction is the fragile path — avoid it). Confirm encoding in Task 2.
- **Ordering:** exact element-by-element if the EEST schema mandates order; set-equality with a set-diff diagnostic otherwise. Decided from Task 2 data.
- **Missing corpus = hard fail:** if the fixtures cache dir is absent, `t.Fatalf`. Note `TestMatcher.Walk` itself does `t.Skip("missing test files")` on a missing dir — so the `os.Stat`+`t.Fatalf` guard MUST run *before* `Walk` is ever called, or the suite silently skips (an implicit mute, forbidden).
- **Unsupported fork = hard fail:** fixtures are `network: "Amsterdam"`; `BlockTest.Run` returns `testforks.UnsupportedForkError` if it's not in `testforks.Forks`. That error must surface as a real test failure, never silently tolerated.

## Technical Details

- Extracted corpus path: `test-fixtures-cache/eest_zkevm/<tarball-internal-layout>` — confirm the exact `blockchain_tests` subpath in Task 2 (likely `fixtures/blockchain_tests/`).
- **Early schema confirmation available in-repo:** there is already a `blockchain_tests_zkevm/` witness corpus (~93 JSON files) in the `execution/tests/execution-spec-tests` submodule on `main`. Task 2 can inspect these immediately to confirm schema (`executionWitness.headers[]` = RLP-hex strings; keys `state`/`codes`/`headers`) without waiting on the 221MB download. The downloaded v0.4.0 tarball is still the corpus the suite runs against; this is only to de-risk the parser/comparison early.
- Fixture JSON: standard EEST blockchain-test object plus a per-block `executionWitness` object — exact field names/encoding confirmed in Task 2.
- `ExperimentalBAL`: Amsterdam fixtures likely require block-access-list support; set `bt.ExperimentalBAL = true` (via existing `WithExperimentalBAL` path) if the corpus needs it (confirm from a fixture's `network`/contents).
- Commitment history: enable schema globally for the test (restore in cleanup) AND write the per-DB flag, since the test DB is built from scratch.

## What Goes Where

- **Implementation Steps** (`[ ]`): worktree, schema verification, manifest/Makefile/test-groups, the seam, parser, runner, CI + caching, build/lint/run.
- **Post-Completion** (no checkboxes): triage of any failing fixtures (file tracking issues — human decision), opening the PR, deciding whether the suite stays in the `-race` matrix or moves to a dedicated lighter workflow if too slow.

## Implementation Steps

### Task 1: Create fresh worktree off origin/main

**Files:** (none — git/worktree setup)

- [x] `git -C /Users/awskii/org/wrk/erigon fetch origin`
- [x] create worktree: `git -C /Users/awskii/org/wrk/erigon worktree add -b awskii/eest-zkevm-witness ../erigon-eest-zkevm origin/main`
- [x] confirm clean tree on `awskii/eest-zkevm-witness` based at `origin/main`
- [x] all subsequent file paths below are relative to the new worktree root

### Task 2: Verify the v0.4.0 fixture witness schema (BLOCKING — do before runner/parser)

**Files:** (none — investigation; record findings in this plan)

- [ ] FAST PATH: inspect the in-repo submodule corpus `execution/tests/execution-spec-tests/blockchain_tests_zkevm/*.json` first to confirm the `executionWitness` shape without downloading (headers expected as RLP-hex strings; keys `state`/`codes`/`headers`)
- [ ] download the v0.4.0 tarball once to a scratch dir and sha256-verify against `f1d6dbec741e325a08d1227c2292be1aa228a78003d6d62b4ab5b53aefc8480a` (also record real size to confirm `221172523`)
- [ ] extract and locate `blockchain_tests` subpath; record the exact path under the extracted root
- [ ] open one v0.4.0 fixture; confirm the per-block `executionWitness` shape matches the submodule corpus: exact field names (`state`/`codes`/`keys`/`headers`?), `headers` encoding (RLP-hex vs JSON objects), element ordering semantics
- [ ] record which fork/`network` the fixtures use and whether `ExperimentalBAL` is required
- [ ] verify `Amsterdam` is registered in `testforks.Forks` (`execution/tests/testforks/forks.go`); if absent, that is a real blocker to surface — every fixture would return `UnsupportedForkError`
- [ ] ✍️ write findings into "Technical Details" above so Tasks 5–6 build on real data, not assumptions

### Task 3: Add fixture to manifest, Makefile target, and CI shard

**Files:**
- Modify: `test-fixtures.json`
- Modify: `Makefile`
- Modify: `tools/test-groups`

- [ ] add `eest_zkevm` key to `test-fixtures.json` (url/sha256/size from Context — only after Task 2 confirms the values)
- [ ] add `test-fixtures-zkevm` Makefile target: `tools/test-fixtures.sh test-fixtures.json test-fixtures-cache eest_zkevm` (mirror the `test-fixtures-eest` target's style + help comment)
- [ ] **CRITICAL ordering:** insert `("execution-eest-zkevm", {"./execution/tests/eest_zkevm_witness/..."})` in `tools/test-groups` GROUPS **before** the `execution-tests` entry. `partition()` claims packages greedily in `OrderedDict` order, and `execution-tests` owns `./execution/tests/...` (a parent of the new package) — if placed after, the new group resolves EMPTY and the suite silently runs inside `execution-tests`
- [ ] verify partitioning: `./tools/test-groups packages execution-eest-zkevm` prints the new package, AND `./tools/test-groups packages execution-tests` does NOT contain it
- [ ] verify `tools/test-groups names` lists `execution-eest-zkevm`, and `make test-fixtures-zkevm` downloads+extracts to `test-fixtures-cache/eest_zkevm/`
- [ ] verify `make test-group TEST_GROUP=execution-eest-zkevm` resolves the new package path (will be empty until Task 6)

### Task 4: Add `BlockTest.RunWithTester` seam

**Files:**
- Modify: `execution/tests/testutil/block_test_util.go`

- [ ] add `func (bt *BlockTest) RunWithTester(t *testing.T) (*execmoduletester.ExecModuleTester, error)` containing the current body of `Run`, returning `(m, nil)` on success and `(nil, err)` on each error path
- [ ] reduce `Run` to `_, err := bt.RunWithTester(t); return err`
- [ ] one-line doc note that the returned tester's lifetime is bound to `t` and callers MUST NOT `Close` it
- [ ] leave `RunCLI` (the parallel non-`*testing.T` copy at ~line 262) deliberately UNTOUCHED — out of scope; the witness runner only needs the `*testing.T` path
- [ ] confirm no behavior change for existing callers (`go build ./execution/tests/...`)
- [ ] run existing block-test package compile/vet to confirm the refactor is inert: `go vet ./execution/tests/testutil/...`

### Task 5: Add `WitnessBlockTest` parser

**Files:**
- Create: `execution/tests/testutil/witness_block_test_util.go`

- [ ] define `WitnessBlockTest` embedding `BlockTest` plus a parallel struct capturing per-block `executionWitness` (field names per Task 2)
- [ ] implement `UnmarshalJSON` that calls `BlockTest.UnmarshalJSON` then unmarshals the witness data
- [ ] define `ExpectedWitness` struct matching the confirmed fixture schema
- [ ] add accessors `NumBlocks() int` and `ExpectedWitnessForBlock(i int) *ExpectedWitness` (nil when block has no witness / out of range)
- [ ] confirm it compiles: `go build ./execution/tests/testutil/...`

### Task 6: Implement the strict witness runner

**Files:**
- Create: `execution/tests/eest_zkevm_witness/testmain_test.go`
- Create: `execution/tests/eest_zkevm_witness/witness_test.go`

- [ ] `testmain_test.go`: `func TestMain(m *testing.M) { testutil.RunTestMain(m) }`
- [ ] `witness_test.go` `TestExecutionSpecWitness`: enable historical commitment via `statecfg.EnableHistoricalCommitment()` and restore prior schema in `t.Cleanup`
- [ ] resolve fixtures dir under `test-fixtures-cache/eest_zkevm/...` (path from Task 2); `os.Stat` + `t.Fatalf` with a "run `make test-fixtures-zkevm`" message if the dir is missing — **this guard MUST run before `Walk`**, because `Walk` does its own `t.Skip("missing test files")` (an implicit mute we must not hit)
- [ ] `Walk` the corpus; per fixture set `ExperimentalBAL` if Task 2 requires it, call `RunWithTester` (real assertion — block-execution failure AND `testforks.UnsupportedForkError` are test failures, NOT absorbed or silently continued)
- [ ] build `DebugAPIImpl` from the returned tester (`NewBaseApi(...)` + `NewPrivateDebugAPI(...)`); write `rawdb.WriteDBCommitmentHistoryEnabled(tx, true)` into the test DB
- [ ] for each block with expected witness: call `ExecutionWitness`; assert non-nil result and compare `State`/`Codes` (exact byte-slice arrays) and `Headers` (read the RPC map's embedded `hash` field; compare to the fixture header decoded RLP-hex → `types.Header` → `.Hash()`); emit informative diffs on mismatch
- [ ] confirm: NO `t.Skip`/`bt.Fails`/`SkipLoad`/build-tags/env gates anywhere in the package, and NO silent tolerance of `UnsupportedForkError`
- [ ] `go build ./execution/tests/eest_zkevm_witness/...`

### Task 7: CI activation — caching + fixture download in the race workflow

**Files:**
- Modify: `.github/workflows/test-all-erigon-race.yml`

- [ ] do NOT add `test-fixtures-cache/**` to `git restore-mtime`: that path is gitignored/untracked (`.gitignore:29`), so `git restore-mtime` is a no-op on it and cannot stabilize mtimes. The plan's earlier "restore-mtime" idea is dropped.
- [ ] Go test-result caching for this package: this is the first `go test` package to read `test-fixtures-cache/` at runtime (existing EEST suites run via the `evm` binary, not `go test`, so they never hit this). Choose one and record it: (a) accept no test-result caching for this heavy, conditionally-run shard (simplest), or (b) add an explicit `touch -t` mtime-normalization step over the extracted dir after `make test-fixtures-zkevm` (mirror the `git submodule foreach ... touch -t` pattern already in `setup-erigon/action.yml`)
- [ ] in `test-all-erigon-race.yml`, add a conditional step (runs only when `matrix.test-group == 'execution-eest-zkevm'`) that restores/saves an `actions/cache` keyed `test-fixtures-zkevm-${{ runner.os }}-${{ hashFiles('test-fixtures.json') }}` over `test-fixtures-cache/*.tar.gz` (tarball only — `test-fixtures.sh` re-extracts), then runs `make test-fixtures-zkevm` before the existing `make test-group` step
- [ ] ensure cross-platform shell per CI-GUIDELINES.md (bash default already set); read `CI-GUIDELINES.md` before editing
- [ ] validate workflow YAML syntax (e.g. `actionlint` if available, else careful manual review)
- [ ] ⚠️ risk note: heavy suite (fresh MDBX per fixture, serial) under `-race`/60m — if runtime is excessive, propose a dedicated lighter non-race workflow instead (decision recorded in Post-Completion)

### Task 8: Build, lint, and run the full corpus

**Files:** (none — verification)

- [ ] `make test-fixtures-zkevm`
- [ ] `make lint` — run repeatedly until clean (non-deterministic)
- [ ] `make erigon integration`
- [ ] `go test ./execution/tests/eest_zkevm_witness/...` (full corpus, no `-short`)
- [ ] record actual pass/fail counts; for each failing fixture, record name + failure reason under ⚠️ for user triage (do NOT mute)
- [ ] report results to the user

### Task 9: Verify acceptance criteria

- [ ] all Overview requirements implemented (manifest, Makefile, shard, seam, parser, runner, CI)
- [ ] confirm zero failure-muting constructs in the diff (`grep` for `Skip`/`Fails`/build-tags in the new files)
- [ ] full suite run completed and results reported
- [ ] `make lint` clean; `make erigon integration` green

### Task 10: Documentation & plan close-out

**Files:**
- Modify: `docs/plans/20260527-eest-zkevm-witness.md` (this file)

- [ ] note any new pattern in `CLAUDE.md` only if genuinely new (likely none — reuses existing machinery)
- [ ] move this plan to `docs/plans/completed/`
- [ ] commit message convention: package prefix, e.g. `execution/tests: add zkevm execution-witness suite (zkevm@v0.4.0)`

## Post-Completion

*Items requiring manual intervention or external systems — informational only*

**Triage (human decisions, after the corpus run):**
- For each failing fixture, decide: implement the missing behavior (e.g. Amsterdam EIPs 7708/7778/7843/7928), or — as a human contributor — add a documented, tracking-issue-linked skip per CLAUDE.md's two-valid-reasons rule. The agent does not add skips.
- File tracking issues for genuine gaps; link them in any human-added skip.

**CI tuning:**
- If the suite is too slow/expensive in the `-race` matrix, move `execution-eest-zkevm` to a dedicated non-race workflow (model on `test-eest-spec.yml`) and exclude it from the auto-matrix.

**PR:**
- Open PR against `main` (new feature). Title prefix with package(s). Mention which workflow runs the new shard and link a run.
