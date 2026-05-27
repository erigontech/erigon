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

### ✅ Task 2 findings (confirmed against the real v0.4.0 tarball — 2026-05-27)

- **Tarball verified:** downloaded asset is **exactly** size `221172523` and sha256 `f1d6dbec741e325a08d1227c2292be1aa228a78003d6d62b4ab5b53aefc8480a` — both Context values are correct; use them verbatim in `test-fixtures.json` (Task 3).
- **Internal layout:** tarball extracts to `fixtures/blockchain_tests/...`. So the extracted corpus path the runner (Task 6) walks is `test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests`. Two top-level subtrees: `for_amsterdam/` and `for_bpo2toamsterdamattime15k/`. **2871 JSON files total.**
- **In-repo submodule fast-path was NOT available:** `execution/tests/execution-spec-tests/blockchain_tests_zkevm/` does not exist in this worktree (only the `legacy-tests` submodule is present, and it is uninitialized). Schema was confirmed directly from the downloaded v0.4.0 corpus instead — strictly better, since it is the exact corpus the suite runs against.
- **Fixture JSON shape:** standard EEST blockchain-test object. Top-level test-object keys: `_info`, `blocks`, `config`, `genesisBlockHeader`, `genesisRLP`, `lastblockhash`, `network`, `postState`, `pre`, `sealEngine`. One file holds **multiple** test objects (one per pytest parametrization); the top key is the `tests/...py::test_x[...]` id.
- **Per-block keys:** `blockAccessList`, `blockHeader`, `blocknumber`, `executionWitness`, `receipts`, `rlp`, `statelessInputBytes`, `statelessOutputBytes`, `transactions`, `uncleHeaders`, `withdrawals`.
- **`executionWitness` shape (CONFIRMED):** a JSON object with **exactly three keys — `state`, `codes`, `headers`** — each an **array of `0x`-prefixed RLP-hex strings**. There is **NO `keys` field anywhere in the corpus** (matches the RPC, where `Keys` is always null). `headers[]` entries are **RLP-hex strings** (e.g. `0xf90279a0…`, an RLP list = encoded `types.Header`), **not** JSON objects — so the Task 79 design holds: decode the fixture RLP-hex → `types.Header` → `.Hash()` and compare to the RPC map's embedded `hash`. In sampled fixtures `headers` had at most **1** element (the parent header); `state` and `codes` are small (single-digit lengths).
- **Some blocks have NO `executionWitness`** (observed e.g. a trailing block in one `eip4895_withdrawals` parametrization, with no `expectException` either). The parser's `ExpectedWitnessForBlock(i)` MUST return `nil` for such blocks and the runner MUST skip the witness assertion for them (not fail).
- **Networks present:** `for_amsterdam/` fixtures are `network: "Amsterdam"`; `for_bpo2toamsterdamattime15k/` fixtures are `network: "BPO2ToAmsterdamAtTime15k"`. **Both are registered** in `execution/tests/testforks/forks.go` (`Forks["Amsterdam"]` line 218, `Forks["BPO2ToAmsterdamAtTime15k"]` line 214). **No `UnsupportedForkError` blocker.**
- **`ExperimentalBAL` REQUIRED:** every sampled block carries a `blockAccessList` → set `bt.ExperimentalBAL = true` (via `WithExperimentalBAL`) in the runner.
- **Ordering / comparison decision:** RPC builds `Codes` as `accessed.SortedCodes` (sorted) and `State` as the trie-node list from `buildWitnessTrie` (`rpc/jsonrpc/debug_execution_witness.go:647,695`); EEST does not document a matching element order for witness trie-node lists. Robust choice for Task 6: compare `state` and `codes` as **multisets** (decode each side to bytes, sort, compare) with a sorted set-diff diagnostic on mismatch — order-of-MPT-nodes is not semantically load-bearing for statelessness. Exact element-by-element is the fragile path; avoid it.
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

- [x] FAST PATH: inspect the in-repo submodule corpus `execution/tests/execution-spec-tests/blockchain_tests_zkevm/*.json` first to confirm the `executionWitness` shape without downloading (headers expected as RLP-hex strings; keys `state`/`codes`/`headers`) — submodule corpus NOT present in this worktree (only `legacy-tests`, uninitialized); confirmed schema directly from the downloaded v0.4.0 corpus instead
- [x] download the v0.4.0 tarball once to a scratch dir and sha256-verify against `f1d6dbec741e325a08d1227c2292be1aa228a78003d6d62b4ab5b53aefc8480a` (also record real size to confirm `221172523`) — both confirmed exact
- [x] extract and locate `blockchain_tests` subpath; record the exact path under the extracted root — `fixtures/blockchain_tests/` (subtrees `for_amsterdam/`, `for_bpo2toamsterdamattime15k/`); recorded in Technical Details
- [x] open one v0.4.0 fixture; confirm the per-block `executionWitness` shape: exact field names (`state`/`codes`/`headers`, NO `keys`), `headers` = RLP-hex strings, comparison-as-multiset decision — recorded in Technical Details
- [x] record which fork/`network` the fixtures use and whether `ExperimentalBAL` is required — `Amsterdam` + `BPO2ToAmsterdamAtTime15k`; `ExperimentalBAL` required (BAL on every block)
- [x] verify `Amsterdam` is registered in `testforks.Forks` (`execution/tests/testforks/forks.go`); if absent, that is a real blocker to surface — every fixture would return `UnsupportedForkError` — both `Amsterdam` (line 218) and `BPO2ToAmsterdamAtTime15k` (line 214) registered; no blocker
- [x] ✍️ write findings into "Technical Details" above so Tasks 5–6 build on real data, not assumptions

### Task 3: Add fixture to manifest, Makefile target, and CI shard

**Files:**
- Modify: `test-fixtures.json`
- Modify: `Makefile`
- Modify: `tools/test-groups`

- [x] add `eest_zkevm` key to `test-fixtures.json` (url/sha256/size from Context — only after Task 2 confirms the values)
- [x] add `test-fixtures-zkevm` Makefile target: `tools/test-fixtures.sh test-fixtures.json test-fixtures-cache eest_zkevm` (mirror the `test-fixtures-eest` target's style + help comment)
- [x] **CRITICAL ordering:** insert `("execution-eest-zkevm", {"./execution/tests/eest_zkevm_witness/..."})` in `tools/test-groups` GROUPS **before** the `execution-tests` entry. `partition()` claims packages greedily in `OrderedDict` order, and `execution-tests` owns `./execution/tests/...` (a parent of the new package) — if placed after, the new group resolves EMPTY and the suite silently runs inside `execution-tests`
- [x] verify partitioning: `./tools/test-groups packages execution-eest-zkevm` prints the new package, AND `./tools/test-groups packages execution-tests` does NOT contain it — confirmed via synthetic package list: zkevm group claims `./execution/tests/eest_zkevm_witness/...`, `execution-tests` does not
- [x] verify `tools/test-groups names` lists `execution-eest-zkevm`, and `make test-fixtures-zkevm` downloads+extracts to `test-fixtures-cache/eest_zkevm/` — names lists it; download sha256-verified ok, extracted to `test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests/` (2871 JSON files, both subtrees)
- [x] verify `make test-group TEST_GROUP=execution-eest-zkevm` resolves the new package path (will be empty until Task 6) — `go list ./... | tools/test-groups packages execution-eest-zkevm` resolves empty (exit 0), as expected pre-Task 6

### Task 4: Add `BlockTest.RunWithTester` seam

**Files:**
- Modify: `execution/tests/testutil/block_test_util.go`

- [x] add `func (bt *BlockTest) RunWithTester(t *testing.T) (*execmoduletester.ExecModuleTester, error)` containing the current body of `Run`, returning `(m, nil)` on success and `(nil, err)` on each error path
- [x] reduce `Run` to `_, err := bt.RunWithTester(t); return err`
- [x] one-line doc note that the returned tester's lifetime is bound to `t` and callers MUST NOT `Close` it
- [x] leave `RunCLI` (the parallel non-`*testing.T` copy at ~line 262) deliberately UNTOUCHED — out of scope; the witness runner only needs the `*testing.T` path
- [x] confirm no behavior change for existing callers (`go build ./execution/tests/...`)
- [x] run existing block-test package compile/vet to confirm the refactor is inert: `go vet ./execution/tests/testutil/...`

### Task 5: Add `WitnessBlockTest` parser

**Files:**
- Create: `execution/tests/testutil/witness_block_test_util.go`

- [x] define `WitnessBlockTest` embedding `BlockTest` plus a parallel struct capturing per-block `executionWitness` (field names per Task 2)
- [x] implement `UnmarshalJSON` that calls `BlockTest.UnmarshalJSON` then unmarshals the witness data
- [x] define `ExpectedWitness` struct matching the confirmed fixture schema (`State`/`Codes`/`Headers` as `[]hexutil.Bytes`, no `keys`)
- [x] add accessors `NumBlocks() int` and `ExpectedWitnessForBlock(i int) *ExpectedWitness` (nil when block has no witness / out of range)
- [x] confirm it compiles: `go build ./execution/tests/testutil/...` (also `go vet` + targeted `golangci-lint`: 0 issues)

### Task 6: Implement the strict witness runner

**Files:**
- Create: `execution/tests/eest_zkevm_witness/testmain_test.go`
- Create: `execution/tests/eest_zkevm_witness/witness_test.go`

- [x] `testmain_test.go`: `func TestMain(m *testing.M) { testutil.RunTestMain(m) }`
- [x] `witness_test.go` `TestExecutionSpecWitness`: enable historical commitment via `statecfg.EnableHistoricalCommitment()` and restore prior schema in `t.Cleanup` (snapshots `statecfg.Schema.CommitmentDomain`, restored in cleanup that runs after the parallel-free corpus walk)
- [x] resolve fixtures dir under `test-fixtures-cache/eest_zkevm/...` (resolved via `runtime.Caller` → `../../../test-fixtures-cache/eest_zkevm/fixtures/blockchain_tests`, CWD-independent); `os.Stat` + `t.Fatalf` with a "run `make test-fixtures-zkevm`" message if the dir is missing — guard runs **before** `Walk`
- [x] `Walk` the corpus (`tm.NoParallel = true` — serial, fresh MDBX per fixture); per fixture set `ExperimentalBAL = true`, call `RunWithTester` (real assertion — block-execution failure AND `testforks.UnsupportedForkError` surface via `t.Fatalf`, never absorbed)
- [x] build `DebugAPIImpl` from the returned tester (`NewBaseApi(nil, m.StateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0, 0)` + `NewPrivateDebugAPI(base, m.DB, nil, 0, false)`); write `rawdb.WriteDBCommitmentHistoryEnabled(rwTx, true)` into the test DB
- [x] for each block with expected witness (block number from the new `BlockNumberForBlock` accessor): call `ExecutionWitness`; assert non-nil result and compare `State`/`Codes` as **multisets** (sorted byte-slice arrays) and `Headers` by embedded `hash` (decode fixture RLP-hex → `types.Header` → `.Hash()`, compare hash multisets); informative sorted-hex diffs on mismatch
- [x] confirm: NO `t.Skip`/`bt.Fails`/`SkipLoad`/build-tags/env gates anywhere in the package (only a comment referencing Walk's own skip), and NO silent tolerance of `UnsupportedForkError`
- [x] `go build ./execution/tests/eest_zkevm_witness/...` — builds; `go vet` clean; targeted `golangci-lint` 0 issues; smoke-ran 1-block (`extcodehash_of_empty`) and 2-block (`withdrawing_to_precompiles`) fixtures green

### Task 7: CI activation — caching + fixture download in the race workflow

**Files:**
- Modify: `.github/workflows/test-all-erigon-race.yml`

- [x] do NOT add `test-fixtures-cache/**` to `git restore-mtime`: that path is gitignored/untracked (`.gitignore:29` = `/test-fixtures-cache/`), so `git restore-mtime` is a no-op on it and cannot stabilize mtimes. The plan's earlier "restore-mtime" idea is dropped — confirmed and not added.
- [x] Go test-result caching for this package: **DECISION — option (a): accept no test-result caching for this heavy, conditionally-run shard.** Rationale: the zkevm runner's deep transitive import graph rebuilds the test binary on nearly every relevant change, so the test-result cache would almost always miss regardless of fixture mtimes — the `touch -t` normalization of option (b) would buy little for a fresh-MDBX-per-fixture serial suite. No mtime-normalization step added; recorded as a comment in the workflow.
- [x] in `test-all-erigon-race.yml`, add a conditional step (runs only when `matrix.test-group == 'execution-eest-zkevm'`) that restores/saves an `actions/cache@v5` keyed `test-fixtures-zkevm-${{ runner.os }}-${{ hashFiles('test-fixtures.json') }}` over `test-fixtures-cache/eest_zkevm.tar.gz` (tarball only — `test-fixtures.sh` re-extracts), then runs `make test-fixtures-zkevm` before the existing `make test-group` step — mirrors `test-eest-spec.yml`'s cache step; both new steps gated on the matrix group so other shards are unaffected
- [x] ensure cross-platform shell per CI-GUIDELINES.md (bash default already set); read `CI-GUIDELINES.md` before editing — read; added steps use the platform-agnostic `actions/cache` action and `make test-fixtures-zkevm` under the workflow's `shell: bash` default
- [x] validate workflow YAML syntax (e.g. `actionlint` if available, else careful manual review) — `actionlint` unavailable; validated via `yq` and `ruby -ryaml` (both parse OK), plus manual review against the `test-eest-spec.yml` cache pattern
- [x] ⚠️ risk note: heavy suite (fresh MDBX per fixture, serial) under `-race`/60m — if runtime is excessive, propose a dedicated lighter non-race workflow instead (decision recorded in Post-Completion). Risk acknowledged; the "CI tuning" fallback in Post-Completion (move `execution-eest-zkevm` to a dedicated non-race workflow modeled on `test-eest-spec.yml`) covers it. Decision deferred to the first real CI run's measured wall time.

### Task 8: Build, lint, and run the full corpus

**Files:** (none — verification)

- [x] `make test-fixtures-zkevm` — cached + sha256-verified (`f1d6dbec741e`), 2871 JSON files
- [x] `make lint` — clean (0 issues), re-run after the runner fix below
- [x] `make erigon integration` — both binaries built green
- [x] `go test ./execution/tests/eest_zkevm_witness/...` (full corpus, no `-short`, `-timeout 0`) — ran in ~1083s, serial, fresh MDBX per fixture
- [x] record actual pass/fail counts; for each failing fixture, record name + failure reason under ⚠️ for user triage (do NOT mute) — see "## Task 8 results" below
- [x] report results to the user — see final summary

➕ **Runner correctness fix made during this run (not a mute):** the first run produced 972 false failures (`block index N has a witness but no parseable block number`). Root cause: invalid-block fixtures (`expectException`) carry an `executionWitness` (the stateless-verifier *input*) but no canonical `blocknumber`, since the block is rejected on import. `debug_executionWitness` cannot be queried for a non-canonical/rejected block, so comparing it is a category error. Verified across the corpus: 22257 witness-blocks have a `blocknumber`; 972 have none, and **all 972 also carry `expectException`** (0 with neither). `BlockTest.RunWithTester` already asserts those blocks are correctly rejected, so validity coverage is unchanged. Fix: capture `expectException` per block (`WitnessBlockTest.BlockExpectsException`) and skip the witness query for those blocks (the `BlockNumberForBlock` `t.Fatalf` is kept as a genuine safety net for the now-empty "valid block, unparseable number" case). Files: `execution/tests/testutil/witness_block_test_util.go`, `execution/tests/eest_zkevm_witness/witness_test.go`.

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

## Task 8 results (full corpus run — 2026-05-27)

**Headline counts** (post runner-fix run, `/tmp/zkevm_witness_run2.log`):

- Subtests: **15425 PASS / 7107 FAIL** (22532 total).
- Distinct fixture files with ≥1 failing subtest: **356 of 2871**.
- Suite is **RED by design** — failures are surfaced, not muted (no `t.Skip`/`Fails`/build-tags/env gates anywhere). Triage is a human decision per CLAUDE.md.
- Wall time: ~1083s locally (non-`-race`, serial, fresh MDBX per fixture). Relevant to the Task-7 `-race`/60m risk note.

**Failure breakdown by reason** (subtest counts):

| Reason | Count | Nature |
|---|---|---|
| `state witness mismatch` | 6825 | Erigon returns a **smaller** state-node set than EEST |
| `codes witness mismatch` | 1176 | code-set differs |
| `ExecutionWitness(...) stateless block execution failed` | 54 | `sender not an eoa` — EIP-7702 delegated-EOA sender during stateless re-exec |
| `headers witness mismatch` | 50 | Erigon returns **fewer** ancestor headers than EEST |
| `state root mismatch after stateless execution` | 2 | stateless re-exec diverges (EIP-7702 delegation clearing) |

⚠️ **Dominant, systematic finding — witness under-population.** In **all 6825** state-witness mismatches Erigon returns *fewer* trie nodes than EEST expects — never more (mean ≈ 13 fewer; deltas span −1…−76). Codes and headers show the same one-directional shortfall. This is not per-EIP noise and not a comparison-harness artifact (multiset compare is content-correct and order-independent): `debug_executionWitness` builds a **strictly smaller** witness than EEST's canonical stateless witness across nearly every feature (`eip7702`, `eip4844`, `eip6780`, `eip1052`, `eip7928`, `eip8025`, `eip8037`, `eip7708`, …). Most likely a single root cause in the witness builder's node/code/header inclusion policy rather than many independent gaps. **For human triage** (do NOT mute): decide whether Erigon's witness builder must include the additional nodes EEST's stateless verifier requires, or whether EEST's completeness definition differs.

⚠️ **EIP-7702 stateless re-exec failures (56 subtests).** `debug_executionWitness`'s stateless re-execution rejects delegated-EOA senders (`sender not an eoa`) and hits 2 post-exec state-root mismatches. Representative fixtures:
- `for_amsterdam/prague/eip7702_set_code_tx/set_code_txs_2/pointer_normal.json`
- `for_amsterdam/prague/eip7702_set_code_tx/set_code_txs_2/call_to_precompile_in_pointer_context.json`
- `for_amsterdam/prague/eip7702_set_code_tx/set_code_txs/delegation_clearing.json` (state-root mismatch)

**Top failing feature dirs** (distinct files): `eip7702_set_code_tx` (58), `eip4844_blobs/excess_blob_gas` (11), `eip6780_selfdestruct` (13), `eip8025_optional_proofs/*` (37 across subdirs), `eip1052_extcodehash` (7), `eip7928_block_level_access_lists` (8). Full per-dir tally is in the run log.

## Post-Completion

*Items requiring manual intervention or external systems — informational only*

**Triage (human decisions, after the corpus run):**
- For each failing fixture, decide: implement the missing behavior (e.g. Amsterdam EIPs 7708/7778/7843/7928), or — as a human contributor — add a documented, tracking-issue-linked skip per CLAUDE.md's two-valid-reasons rule. The agent does not add skips.
- File tracking issues for genuine gaps; link them in any human-added skip.

**CI tuning:**
- If the suite is too slow/expensive in the `-race` matrix, move `execution-eest-zkevm` to a dedicated non-race workflow (model on `test-eest-spec.yml`) and exclude it from the auto-matrix.

**PR:**
- Open PR against `main` (new feature). Title prefix with package(s). Mention which workflow runs the new shard and link a run.
