# Erigon Unit-Test Coverage — Baseline & Tracking

Companion to the coverage-improvement plan. Driven by **per-package** statement
coverage (each package against its own tests only). Cross-package tests are
treated as integration tests and are out of scope here.

## Methodology

```bash
export ERIGON_SKIP_CL_SPECTEST=true
export CGO_CFLAGS="$(go env CGO_CFLAGS) -D__BLST_PORTABLE__ -Wno-unknown-warning-option -Wno-enum-int-mismatch -Wno-strict-prototypes -Wno-unused-but-set-variable -O3"
export CGO_LDFLAGS="$(go env CGO_LDFLAGS) -O3 -g"
go test -covermode=atomic -coverprofile=coverage-baseline.out -timeout 30m ./... 2>&1 | tee coverage-baseline.log
go tool cover -func=coverage-baseline.out > coverage-baseline-func.txt
```

Per-package % is read from the `coverage: NN.N% of statements` token on each
`ok`/`FAIL` line in `coverage-baseline.log`. `[no test files]` packages = 0%.

## Tiers & targets

| Tier | Description | Target |
|------|-------------|--------|
| T1 | Pure logic (encoding, types, rules, math) | 85–95% |
| T2 | Stateful but isolatable (in-mem DB / temp dir) | 70–85% |
| T3 | Integration-heavy (pipeline, network) | best-effort |

**Acceptance gate (all tiers): every error-returning function has a covering
negative-path test.** Line % is necessary but not sufficient.

## Baseline summary

- Date: 2026-06-07
- **Root-module overall statement coverage: 34.5%** (weighted, from
  `go tool cover -func`).
- Packages measured: 430 with coverage rows + 30 `[no test files]` = 460 listed.
- `node/interfaces` submodule: not yet measured (deferred; small).

### Distribution (per-package, 460 packages)

| Bucket | Packages |
|--------|----------|
| 0% (incl. no-test) | 249 |
| <25% | 48 |
| 25–50% | 63 |
| 50–75% | 54 |
| ≥75% | 46 |

The 0% bucket is dominated by `cl/*` (Caplin) packages and `*/mock_services`
/ test-helper dirs. Core EL packages cluster in the 25–75% range.

### Pre-existing test failures (NOT introduced by this work — flagged for the user)

These 4 packages had failing tests during the baseline run. They still emit
partial coverage. Per CLAUDE.md we do **not** skip them — they are recorded as
findings to investigate/escalate separately from coverage work:

- `execution/engineapi` — `TestEngineApiBALMixedBlock`
- `execution/tests` — `TestInvalidReceiptHashHighMgas`
- `rpc/jsonrpc` — `TestSetHead_E2E_ModeB_NoSnapshotTrim`
- `txnprovider/shutter` — `TestShutterBlockBuilding`

(Full sorted per-package list saved to `/tmp/cov-sorted.tsv`; regenerate from
`coverage-baseline.log` any time.)

## First-wave targets — real baseline numbers

Top-level package + notable sub-packages. Tier and target per the framework.

| Package | Tier | Baseline % | Target % | Notes |
|---------|------|-----------|----------|-------|
| execution/types | T1 | **46.9%** | 90% | subpkgs: accounts 22.7%, stateless 36.5%, ethutils 0% |
| execution/rlp | T1 | **80.4%** | audit-only | already high; internal/rlpstruct 0% — audit gaps only |
| execution/protocol (root) | T1 | **26.0%** | 85% | — |
| execution/protocol/rules | T1 | **0.0% (no tests)** | 85% | consensus rule helpers — high value |
| execution/protocol/params | T1 | **0.0% (no tests)** | best-effort | mostly constants — test only behavior, not values |
| execution/protocol/misc | T1 | 20.8% | 85% | gas/opcode helpers |
| execution/protocol/rules/merge | T1 | 6.8% | 85% | — |
| db/kv (root) | T2 | **0.0%** | 75% | critical KV abstraction; helpers dbutils/memdb/bitmapdb/order also 0% |
| db/etl | T2 | **72.3%** | 80% | close — fill -ve paths |
| db/snaptype | T1 | **16.8%** | 85% | — |
| db/snaptype2 | T1 | **3.5%** | 85% | — |
| execution/commitment (root) | T2 | **59.2%** | 75% | trie 51.7%, commitmentdb **4.9%**, nibbles 91.7% |
| db/state | T2 | **60.7%** | 75% | changeset 36.8%, statecfg 26.0%, stats 0% |
| execution/exec | T3 | **2.8%** | best-effort | needs DB/state harness |
| execution/execmodule | T3 | **40.1%** | best-effort | execmoduletester 51.7%, moduleutil 61.8%, chainreader 0% |

### Declaration-only packages — EXCLUDED from targets (no testable behavior)

A 0% package is not automatically a target. Some are 0% because they contain
**only declarations** — interfaces, type/const definitions, sentinel `error`
vars — with zero executable statements. Testing them would assert *values*
(e.g. an error string), which violates the "test behavior, not values"
principle. Exclude unless/until they gain real logic:

- `execution/protocol/rules` — interfaces + `RewardKind` consts + sentinel
  error vars only. **Excluded.**
- `execution/protocol/params` — constants. **Excluded** (behavior that *uses*
  these params is tested in `misc`/`rules/*`, not here).
- Check each 0% package for a real testable surface before adding it.

### Suggested Wave A order (real behavior + rich -ve cases first)

1. `execution/types/accounts` (22.7%) — account ser/de: round-trip + malformed
   /short-buffer decode (-ve) cases. **First concrete package.**
2. `execution/protocol/misc` (20.8%) + `rules/merge` (6.8%) — pure gas/diff
   helpers.
3. `execution/types` codec paths (46.9% root).
4. `db/snaptype2` (3.5%) + `db/snaptype` (16.8%) — filename/type parsing, lots
   of -ve cases.
5. `execution/rlp` audit (80.4% → close residual gaps, e.g. internal/rlpstruct).

## Wave A progress

| Package | Before | After | Lint | Notes |
|---------|--------|-------|------|-------|
| execution/types/accounts | 22.7% | **93.5%** | clean | +2 test files (codec + key_types), all error branches have -ve tests; comment-hygiene: fixed copy-pasted "hash"→"address" docs, removed dead commented-out block in DeserialiseV3. Remaining gaps are unreachable/deep RLP branches (l>128 encode, multi-byte length prefixes). |
| execution/protocol/misc | 20.8% | **54.8%** | clean | +3 test files covering all pure-logic functions (blob-gas math, Cancun header presence/absence, deposit-log parse/validate, ETH transfer/burn log builders, PoS header, DAO extra-data, gaslimit verify/calc, eip1559 verify) with full -ve coverage; comment-hygiene: removed dead commented log.Debug + commented t.Logf. Remaining 0% funcs are state/syscall-dependent (ApplyDAOHardFork, CurrentFees, eip2935 store, eip4788, eip7002/7251 dequeue, Transfer) — **T2/T3, deferred** (need IntraBlockState/kv harness). |
| execution/protocol/rules/merge | 6.8% | **12.5%** | clean | **T3** consensus-engine wrapper. Added header-validation -ve tests (verifyHeader 13%→35%: extra-data, block time, gas limit/used, block number, uncle hash) + TxDependencies, reusing existing readerMock. The verifyHeader Config-dependent tail (Shanghai/Cancun/Prague/Amsterdam/1559/blob branches) and the large Finalize/Initialize/Prepare/CalculateRewards/Seal functions need a full chain-reader + IntraBlockState harness — **deferred**. Package % is low by construction (bulk is state glue). |
| db/snaptype | 16.8% | **38.7%** | clean | +1 test file covering pure filename/type logic with -ve cases: IsCorrectFileName, IsCaplin, IsTorrentPartial, ext helpers, IsSeedableExtension, Hex2InfoHash (incl. panic-on-bad-hex), filename builders, IsStateFile/V2, FileInfo methods (Name/Dir/Base/Len/GetRange/GetType/GetGrouping/CompareTo/As), ParseFileName/ParseRange invalid inputs, ParseFileNameOld, plus a t.TempDir() test for ParseDir/TmpFiles/Segments/IdxFiles (incl. not-exist path). Cleanup: removed a stray debug `println` in ParseFileNameOld. Remaining gaps are index-building/extract functions needing a seg/DB harness. |
| db/snaptype2 | 3.5% | — | — | **Deferred (no pure surface).** Package is mostly snapshot-type *registration data* (vars) plus file/DB functions: TxsAmountBasedOnBodiesSnapshots (needs a built seg.Decompressor) and the HeaderFreezer methods (need a kv DB + collector). No meaningful pure unit tests without a snapshot-file/DB harness — revisit in a later T2 pass. |

## Change log

- 2026-06-07: doc created; baseline run launched.
- 2026-06-07: baseline complete — overall **34.5%**; first-wave numbers
  recorded; 4 pre-existing failures flagged.
- 2026-06-07: `execution/types/accounts` 22.7% → 93.5% (Wave A package 1).
