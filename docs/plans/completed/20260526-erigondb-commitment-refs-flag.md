# erigondb.toml as Source of Truth for Commitment Branch Referencing

## Overview

Move the commitment "references in branches" setting from a build-time constant
(`AggregatorSqueezeCommitmentValues`) into `erigondb.toml`, so a node inherits it from
the snapshot set it runs on rather than from the binary. Default is `true`. This replaces
the `awskii/36exp` const-flip hack.

The hard part is correctness, not configuration. Today read/expand and write/shorten are
**symmetric** — both gate on the same flag. Once the flag can be `false` while referenced
files exist on disk, that symmetry corrupts data:

- **Read** of an existing referenced (≥threshold) file with the flag off → short keys are
  read as plain → garbage.
- **Merge** of referenced inputs with the flag off → `commitmentValTransformDomain`
  (`db/state/domain_committed.go:297`) returns `valBuf` *as-is*, copying input short keys
  (offsets into the soon-deleted input files) into the merged file → stale offsets → unreadable.

The fix decouples the two decisions:
- **expand-on-read** is a property of the *input file* (was it written referenced?), keyed
  off the file's **version**, not the live flag.
- **re-shorten-on-write** is governed by the live flag.

A commitment `.kv` file's version is the per-file regime marker: `< v2.1` (+ range ≥ threshold)
⇒ referenced; `v2.1` ⇒ plain. Reads consult per-file version, so flipping the flag on a
populated datadir stays correct in both directions, and old referenced files convert to plain
lazily through normal merges (no upfront migration).

**Key benefit:** snapshot producers set `references_in_commitment_branches = false` in the
datadir's `erigondb.toml` before running; merges then produce plain `v2.1` files and that toml
ships with the snapshot set, so consumers inherit `false`. No build divergence, no CLI flag.

**CRITICAL — must land together:** the toml-flag plumbing and the version-aware read/merge are
one cohesive change. Shipping the flag without the version-aware read/merge is unsafe (a flag
flip would corrupt reads). All tasks below land in a single PR.

## Context (from discovery)

- **Settings machinery (existing pattern):** `db/state/erigondb_settings.go` —
  `ErigonDBSettings` struct + `ResolveErigonDBSettings` (3-case resolution) +
  `Aggregator.ReloadErigonDBSettings` (`db/state/aggregator.go:313`). `step_size` /
  `steps_in_frozen_file` already flow this way; the new field slots in identically.
- **The constant:** `AggregatorSqueezeCommitmentValues = true` (`db/state/statecfg/state_schema.go:76`),
  seeds `CommitmentDomain.ReplaceKeysInValues` in the `Schema` literal (`state_schema.go:262`).
- **The flag field — read/expand site:** `db/state/domain_committed.go:52` (deref), `:297` (merge closure body).
- **The flag field — BEHAVIORAL merge-scheduling guards (not renames):** `db/state/aggregator.go:1832`
  (range-alignment hold), `:1851` (commitment range coordination), `:1935`/`:1941` (whether the
  `vt` transformer is created *at all* + `accStorageMerged.Wait()`). With the flag off and
  referenced inputs present, `:1941` skips the transformer, so `mergeFiles` writes input short
  keys verbatim → the corruption this plan exists to prevent. These must gate on "are inputs
  referenced", not the write flag.
- **The flag field — other sites:** `db/state/squeeze.go:125` (read), `:345` (log string only),
  `:780`/`:1059` (rebuild global-schema reads), and the `ForTestReplaceKeysInValues` toggles at
  `:443` (disable before rebuild loop), `:797`/`:1066` (re-enable for squeeze pass), `:832`
  (disable during rebuild).
- **Threshold (stays a build const):** `minStepsForReferencing = 2`,
  `ValuesPlainKeyReferencingThresholdReached(stepSize, from, to)` (`db/state/domain_committed.go:38-44`);
  also called from `db/integrity/commitment_integrity.go:514,1266,1961`.
- **File version:** `db/state/statecfg/version_schema_gen.go:24` — on `main`,
  commitment `DataKV = {Current: v2.0, MinSupported: v1.0}`. (The v2.1 bump lives only on
  `awskii/36exp`; bumping it is part of this work.) `Versions.MustSupport`
  (`db/version/file_version.go:194`) panics if a file version is outside `[MinSupported, Current]`,
  so reading v2.1 files **requires** `Current ≥ v2.1`.
- **Per-file version is NOT on `FilesItem`** (`db/state/dirty_files.go:116`); the domain-file scan
  that builds the dirty `FilesItem` set is `Domain.openDirtyFiles` (`db/state/dirty_files.go:387`),
  which already parses `fileVer` via `version.MatchVersionedFile` and calls
  `DataKV.MustSupport(fileVer, fName)` at `:402`. The parsed `version.Version` (singular) must be
  stored on `FilesItem` there. (Note: `aggregator2.go:213` is a *block-snapshot* scan, not this path.)
- **Filename version source:** `db/state/domain.go:139` builds the `.kv` name from
  `d.FileVersion.DataKV.String()` (= `Current`) — generic across domains; commitment needs a
  flag-derived override.
- **Generated version file:** `db/state/statecfg/version_schema_gen.go` is `// Code generated by
  bumper; DO NOT EDIT.` — `db/state/statecfg/versions.yaml` is the source; regenerate via `make gen`.
- **Deref entry doesn't carry version:** `replaceShortenedKeysInBranch` (`domain_committed.go:48`)
  receives only `prefix, branch, fStartTxNum, fEndTxNum`; callers (`aggregator.go:~2488,2499` via
  `getLatestFromFiles`, which returns `fileStartTxNum/fileEndTxNum` but no version) don't pass a
  version. It already range-matches commitment files at `:74-82` (metrics) — reuse that to fetch
  the Task-5 per-file version, or extend `getLatestFromFiles` to return it.
- **Key remap API:** `BranchData.ReplacePlainKeys` (`execution/commitment/commitment.go:808`);
  the `fn` callback determines direction (expand offsets→plain, or plain→offsets).

## Development Approach

- **Testing approach: TDD (tests first)** — mandated by `CLAUDE.md` (Red → Green → Refactor).
  Automated agents must **never** add `t.Skip` (any form) — investigate/fix or escalate.
- Complete each task fully (code + tests green) before the next.
- For the rename task only (mechanical, no behavior change): existing tests are the safety net;
  state this in the PR. All other tasks require new/updated tests.
- Run `make lint` (non-deterministic — repeat until clean) and `make erigon integration`
  before committing.
- Branch: `awskii/erigondb-commitment-refs-flag` (NOT the `alex/...` pattern). Base: `main`.
- Comments: default none; if required, one sentence, high-level, no forensic detail.
- New files (e.g. `erigondb_settings_test.go`, `domain_committed_test.go`) use a 2026 copyright header.

## Testing Strategy

- **Unit tests:** required every task (settings resolution, version predicate, transformer).
- **Correctness/integration tests:** a datadir holding *both* v2.0-referenced and v2.1-plain
  commitment files reads both correctly; merging v2.0 referenced inputs with the flag off
  yields a readable v2.1 plain output (assert no stale offsets); flipping the flag in both
  directions stays correct.
- **No UI / e2e** in this repo for this area.
- Extend the `erigondb-sync-integration-test-plan` scenarios (legacy / fresh+downloader /
  fresh+no-downloader) to assert the new field resolves correctly per scenario.

## Progress Tracking

- mark completed items `[x]` immediately
- ➕ prefix for newly discovered tasks
- ⚠️ prefix for blockers
- keep this file in sync with actual work

## Solution Overview

Three independent signals cooperate:

1. **File version** (`DataKV`): regime marker. Stamp `v2.0` when writing referenced, `v2.1`
   when writing plain. Read ceiling `Current = v2.1` (accept all); `MinSupported = v1.0`.
2. **Threshold** (`minStepsForReferencing`, build const): within the referenced regime, only
   files whose range ≥ threshold actually carry short keys.
3. **Flag** (`references_in_commitment_branches`, from `erigondb.toml`, default `true`):
   governs *new writes* only (and thus the version stamped). Never gates reads.

Read/deref predicate: `referenced := fileVersion.Less(v2_1) && ValuesPlainKeyReferencingThresholdReached(stepSize, fileFrom, fileTo)`.

Merge: always expand each input branch when its input file is referenced (per its own
version+range); re-shorten into the output only if `flag && outputRange ≥ threshold` (stamp
`v2.0`), else write plain (stamp `v2.1`).

## Technical Details

- `ErigonDBSettings.ReferencesInCommitmentBranches *bool` (`toml:"references_in_commitment_branches"`),
  `nil` = absent → normalized to `true` in memory only (existing-file branch must NOT rewrite a
  downloaded toml — it is synced snapshot metadata).
- `config3.DefaultReferencesInCommitmentBranches = true` replaces `AggregatorSqueezeCommitmentValues`.
- Rename `DomainCfg.ReplaceKeysInValues` → `ReferencesInCommitmentBranches` repo-wide;
  `ForTestReplaceKeysInValues` → `ForTestReferencesInCommitmentBranches`.
- `FilesItem` gains a parsed `version` field, populated where dirty files are scanned.
- Commitment `.kv` **write** version is flag-derived (v2.0/v2.1), decoupled from `Current`.

## What Goes Where

- **Implementation Steps** (checkboxes): all code + tests in this repo.
- **Post-Completion** (no checkboxes): producing/publishing v2.1 snapshot sets, fleet rollout,
  downgrade caveat verification.

## Implementation Steps

### Task 1: Rename `ReplaceKeysInValues` → `ReferencesInCommitmentBranches` (mechanical)

**Files:**
- Modify: `db/state/statecfg/state_schema.go` (field def + literal `:262`)
- Modify: `db/state/domain_committed.go` (`:52`, `:297`)
- Modify: `db/state/aggregator.go` (`:281` helper, `:1831`, `:1851`, `:1921`, `:1935`, `:1941`)
- Modify: `db/state/squeeze.go` (`:125`, `:345`, `:780`, `:1059`, and `ForTest…` toggles `:443`,`:797`,`:832`,`:1066`)
- Modify (ForTest callers): `cmd/integration/commands/commitment.go`, `db/state/aggregator_test.go`,
  `db/state/aggregator_fuzz_test.go`, `db/state/squeeze_test.go`,
  `db/state/squeeze_concurrent_rebuild_test.go`, `db/state/trie_reader_integration_test.go`,
  `db/test/aggregator_ext_test.go`

- [x] rename the `DomainCfg.ReplaceKeysInValues` struct field to `ReferencesInCommitmentBranches`
- [x] rename `ForTestReplaceKeysInValues` → `ForTestReferencesInCommitmentBranches` across **all** callers repo-wide (grep confirms 9 files: aggregator.go def + cmd/integration + 6 test files)
- [x] update read/log sites to the new name (`domain_committed.go:52,297`; `squeeze.go:125,345,780,1059`)
- [x] rename the field references at the merge-scheduling guards `aggregator.go:1831,1851,1921,1935,1941` — **name only here**; their *behavioral* decoupling is Task 8 (do not change logic in this task) — only `:281,:1831,:1921` use the field literally; `:1851/:1935/:1941` use the `commitmentUseReferencedBranches` local, unchanged here
- [x] no new tests (mechanical rename; existing tests are the safety net — note in PR)
- [x] `make erigon integration` builds; existing tests pass before next task

### Task 2: Default constant in config3 + retire `AggregatorSqueezeCommitmentValues`

**Files:**
- Modify: `db/config3/config3.go`
- Modify: `db/state/statecfg/state_schema.go`

- [x] add `const DefaultReferencesInCommitmentBranches = true` to `db/config3/config3.go` with a one-line doc pointing at `erigondb.toml`
- [x] replace `AggregatorSqueezeCommitmentValues` (`state_schema.go:76`) usage; seed the `Schema` literal (`:262`) from `config3.DefaultReferencesInCommitmentBranches`
- [x] remove the now-unused `AggregatorSqueezeCommitmentValues` const (grep confirmed: only used in state_schema.go; no external callers)
- [x] write/adjust a small test asserting the default schema value is `true` (`state_schema_test.go::TestCommitmentReferencesDefault`)
- [x] run tests — must pass before next task

### Task 3: `erigondb.toml` field + resolution semantics

**Files:**
- Modify: `db/state/erigondb_settings.go`
- Create: `db/state/erigondb_settings_test.go` (if absent; else modify)

- [x] add `ReferencesInCommitmentBranches *bool` with `toml:"references_in_commitment_branches"` to `ErigonDBSettings`
- [x] existing-file branch: after unmarshal, normalize `nil → &true` **in memory only**; do not rewrite the file
- [x] legacy branch (preverified present): write explicit `true`
- [x] fresh + `noDownloader`: write explicit `true`; fresh + downloader: leave for the downloader's file (in-memory default `true`)
- [x] add a resolved accessor (e.g. `func (s *ErigonDBSettings) RefsInCommitmentBranches() bool { return s.ReferencesInCommitmentBranches == nil || *s.ReferencesInCommitmentBranches }`) for safe consumption
- [x] write tests: absent→true; explicit `false` honored; explicit `true` honored; legacy writes `true`; fresh+noDownloader writes `true`; existing-file branch does NOT rewrite the file; `*bool` round-trips through marshal/unmarshal
- [x] run tests — must pass before next task

### Task 4: Thread the resolved flag into schema + aggregator

**Files:**
- Modify: `db/state/aggregator.go` (`ReloadErigonDBSettings` `:313`)
- Modify: `cmd/rpcdaemon/cli/config.go` (`:449`), `rpc/rpchelper/commitment.go` (`:75`)
- Modify: `cmd/integration/commands/state_history.go` (`:78`), `stages.go` (`:1078`), `commitment.go`
- Modify: `cmd/utils/app/squeeze_cmd.go` (`:144`,`:190`), `snapshots_cmd.go` (`:3567`), `step_cmd.go` (`:30`)
- Modify: `execution/state/genesiswrite/genesis_write.go` (`:357`), `node/eth/backend.go` (`:1338`)
- Modify: `db/state/aggregator_test.go` / `db/test/*` as needed

- [x] add an apply helper that writes the resolved bool to global `statecfg.Schema.CommitmentDomain.ReferencesInCommitmentBranches` (covers rebuild paths `squeeze.go:780,1059` + next `Configure`) and, when `a.configured`, live `a.d[kv.CommitmentDomain].ReferencesInCommitmentBranches` — `Aggregator.applyReferencesInCommitmentBranches` (`aggregator.go`)
- [x] call the helper from `ReloadErigonDBSettings` (primary site; runs each start before execution via `stage_snapshots.go:213`)
- [x] audit each standalone entry point above: if it reads commitment, apply the resolved flag (else it silently falls back to default `true`) — all aggregator-constructing entry points funnel through `WithErigonDBSettings → Open`, which now captures the resolved flag into `AggOpts.referencesInCommitmentBranches` and applies it before `ConfigureDomains`; `state_history.go` builds only a `History` (no commitment branch read), `step_cmd.go` uses only `StepSize`, and `commitment.go` rebuild explicitly controls the flag itself (Task 9) — no per-site edits needed
- [x] write tests: toml `false` → resolve → assert it lands on global `statecfg.Schema.CommitmentDomain` and on a live `a.d[CommitmentDomain]` — `aggregator_refs_test.go` (`ReloadErigonDBSettings` path + builder `Open` path, false and true)
- [x] run tests — must pass before next task

### Task 5: Carry parsed file version on `FilesItem`

**Files:**
- Modify: `db/state/dirty_files.go` (`FilesItem` struct `:116`; populate in `openDirtyFiles` `:387`/`:402`)
- Modify: `db/state/dirty_files_test.go`

- [x] add a `version version.Version` (singular `Version`, not the `Versions` ceiling pair) field to `FilesItem`
- [x] populate it in `Domain.openDirtyFiles` (`:387`) from the `fileVer` already parsed by `version.MatchVersionedFile` right before the `DataKV.MustSupport(fileVer, fName)` call at `:402` — this is the path that actually builds the dirty `FilesItem` set (NOT `aggregator2.go:213`, which is block snapshots)
- [x] expose the version on `visibleFile` (accessor `visibleFile.Version()`) so the deref/merge sites can read it; it survives `recalcVisibleFiles` because the accessor reads through the preserved `src *FilesItem` pointer (recalc rebuilds the `visibleFile` slice but reuses the same `FilesItem`)
- [x] decide + implement how the version reaches `replaceShortenedKeysInBranch` (Task 7) and the merge transformer (Task 8): **decision** — store on `FilesItem`, reach via the existing range-match loop at `domain_committed.go:74-82` (and `visibleFile.src.version`); do NOT extend `getLatestFromFiles`. The accessor + field make it reachable now; call-site wiring is Task 7/8.
- [x] write tests: scanning a set of files yields items with the correct parsed versions (mix v1.0/v2.0/v2.1); accessor returns the right version through the visible-files layer — `dirty_files_test.go::TestOpenDirtyFilesPopulatesVersion` + `TestVisibleFileVersion`
- [x] run tests — must pass before next task

### Task 6: Accept v2.1 as commitment kv read ceiling; flag-derived write version

**Files:**
- Modify: `db/state/statecfg/versions.yaml` (commitment → domain → kv → `current: v2.1`) — **source of truth**
- Regenerate: `db/state/statecfg/version_schema_gen.go` via `make gen` (it is `// Code generated … DO NOT EDIT` — do not hand-edit)
- Modify: `db/state/domain.go` (`kvNewFilePath`/`:138-139` — commitment write version)
- Modify: `db/state/domain_test.go` / `db/state/dirty_files_test.go`

- [x] set commitment `DataKV.current = v2.1` in `versions.yaml` (keep `min: v1.0`); minor bump, not major (per decision); regenerated the `.go` via `make versions-gen`
- [x] introduce a commitment-kv **write version** selector decoupled from `Current`: `v2.0` when `ReferencesInCommitmentBranches` is on, `v2.1` when off — `Domain.kvWriteVersion` (`domain.go`), used by `kvNewFilePath` for the commitment domain only (other domains keep `DataKV.Current`)
- [x] verify `MustSupport`/scan accepts v2.0 *and* v2.1 commitment files (range `[v1.0, v2.1]`) — covered by `TestCommitmentKvVersionAcceptance`
- [x] write tests: refs-on writes v2.0 filename, refs-off writes v2.1 filename; both v2.0 and v2.1 files pass `MustSupport`; a v2.2 (hypothetical) is rejected — `domain_test.go::TestCommitmentKvWriteVersion`, `TestNonCommitmentKvWriteVersionUsesCurrent`, `TestCommitmentKvVersionAcceptance`
- [x] run tests — must pass before next task

### Task 7: Version-aware read/deref (decouple read from the live flag)

**Files:**
- Modify: `db/state/domain_committed.go` (`replaceShortenedKeysInBranch` `:48-57`)
- Modify: `db/state/domain_committed_test.go` (create if absent)

- [x] obtain the source commitment file's version at the deref site via the Task-5 mechanism — `AggregatorRoTx.commitmentFileVersionByRange` reuses the `:74-82` range-match against `FilesItem.version` (via `visibleFile.Version()`) and folds in the metric-bucket index; did NOT extend `getLatestFromFiles`
- [x] change the deref gate so it no longer reads the live `ReferencesInCommitmentBranches` flag; instead compute `referenced := fileVersion.Less(version.V2_1) && ValuesPlainKeyReferencingThresholdReached(stepSize, fileFrom, fileTo)` via the new `commitmentBranchReferenced` predicate using the **source commitment file's** version and its own range
- [x] keep the existing short-circuits (empty branch, `KeyCommitmentState` prefix, no state files)
- [x] write tests: `domain_committed_test.go::TestCommitmentBranchReferenced` covers v2.0/v1.0 ≥threshold → referenced (flag-independent by construction — predicate takes no flag), v2.1 → never referenced, v2.0 <threshold → plain, hypothetical v2.2 → plain; heavyweight read-path tests (`TestAggregator_RebuildCommitmentBasedOnFiles`, `TestAggregator_SqueezeCommitment`) exercise the actual deref end-to-end
- [x] run tests — must pass before next task

### Task 8: Decouple merge SCHEDULING from the write flag (the corruption vector)

The guards at `aggregator.go:1832/1851/1935/1941` currently gate transformer creation and
account/storage merge-coordination on the live write flag. With the flag off and referenced
(v2.0, ≥threshold) inputs present, `:1941` skips the transformer, so `mergeFiles` copies input
short keys verbatim into the merged file → stale offsets → unreadable. Transformer creation and
coordination must instead trigger whenever **any input commitment file is referenced**,
independent of the write flag.

**Files:**
- Modify: `db/state/aggregator.go` (`findMergeRange` `:1830-1884`, `mergeFiles` `:1920-1955`)
- Modify: `db/state/aggregator_test.go`

- [x] introduce a predicate "the commitment merge has referenced inputs" — any input commitment `FilesItem` with `version.Less(version.V2_1)` AND its range ≥ threshold (uses Task 5 per-file version) — `commitmentMergeInputsReferenced` (resolved inputs) + `commitmentVisibleFilesReferenced` (visible files), both in `domain_committed.go`, reusing the Task-7 `commitmentBranchReferenced` per-file predicate
- [x] note the predicate is evaluated against different file sets at the two points: at `:1832`/`:1851` the candidate merge range is not finalized yet, so scan **all visible commitment files** (`commitmentVisibleFilesReferenced` over `at.d[kv.CommitmentDomain].files`) — the safe over-approximation; at `:1935`/`:1941` use the resolved merge inputs (`commitmentMergeInputsReferenced(files.d[kv.CommitmentDomain], …)`)
- [x] gate the range-alignment hold (`:1832`) and commitment range coordination (`:1851`) on that predicate, not on the write flag — local renamed `commitmentUseReferencedBranches → commitmentMergeReferencing := flag || commitmentVisibleFilesReferenced()` (`||flag` preserves old flag-on behavior; the referenced-inputs term fixes the flag-off corruption path)
- [x] gate `accStorageMerged.Add/Wait` (`:1935-1942`) and `vt` creation (`:1941`) on "referenced inputs present", so the transformer always runs when expansion is needed — even with the write flag off — `commitmentMergeReferencing := flag || commitmentMergeInputsReferenced(...)` drives Add/Wait/Done/vt symmetrically
- [x] when there are no referenced inputs (all v2.1 plain) and the flag is off, keep `vt == nil` (no-op) — the existing fast path (predicate false ⇒ `commitmentMergeReferencing` false ⇒ no transformer)
- [x] write tests (assert **scheduling only** — not output bytes): `aggregator_test.go::TestCommitmentMergeInputsReferenced` (resolved-inputs gate: v2.0/v1.0 ≥threshold→referenced, v2.1→plain, <threshold→plain, v2.2→plain, nil/empty) and `TestCommitmentVisibleFilesReferenced` (planning over-approximation through the visible-files layer with flag off); heavyweight `TestAggregator_RebuildCommitmentBasedOnFiles`/`TestAggregator_SqueezeCommitment` exercise the real merge scheduling end-to-end
- [x] run tests — must pass before next task

### Task 9: Merge transformer body — always expand input, conditionally re-shorten output

**Files:**
- Modify: `db/state/domain_committed.go` (`commitmentValTransformDomain` `:249-320`, closure `:295-320`)
- Modify: `db/state/squeeze.go` (`:241` caller; rebuild toggles `:443,:797,:832,:1066`; gates `:780,:1059`)
- Modify: `db/state/domain_committed_test.go`

- [x] restructure the `vt` closure: (a) expand input short keys → plain whenever the **input** file is referenced (input version < v2.1 AND input range ≥ threshold), even when the flag is off — never return referenced `valBuf` as-is; (b) re-shorten into the merged output only if `ReferencesInCommitmentBranches && outputRange ≥ threshold`, remapping to merged account/storage offsets; else emit plain — gate is `inputReferenced := commitmentBranchReferenced(dt.fileVersionByRange(keyFrom,keyEnd), …)` (expand) + `reshorten := flag && threshold(rng.from,rng.to)` (re-shorten); the replacer returns `nil`/expanded-plain when `!reshorten`
- [x] evaluate the threshold against the *input* file range for the expand decision and the *output* range (`rng.from/to`) for the re-shorten decision (do not conflate them, as the current `:297` did) — `inputReferenced` uses `keyFromTxNum/keyEndTxNum`; `reshorten` uses `rng.from/to`
- [x] ensure the merged commitment file is stamped via the Task 6 flag-derived write-version (v2.0 when re-shortened, v2.1 when plain) — merge output via `kvNewFilePath→kvWriteVersion`; **also fixed `SqueezeCommitmentFiles` to write its re-referenced output to the flag-derived `kvNewFilePath` (v2.0)** instead of reusing the input name (which could be a plain v2.1 rebuild-window file → v2.1 name with referenced content → stale-offset corruption after reload)
- [x] **rebuild/squeeze interaction:** verified end-to-end — `TestCommitmentRebuildSqueezeReadableAfterReload` runs the full flag-off rebuild → flag-on squeeze cycle, reopens from disk (versions parsed from names), and asserts version/content consistency + identical root
- [x] write tests: merging two v2.0 referenced inputs with flag off → v2.1 plain output that reads back correctly (assert expanded plain keys, no stale offsets); with flag on → v2.0 referenced output; mixed referenced+plain inputs correct — `commitment_merge_version_test.go::TestCommitmentMergeFlagOffExpandsReferencedInputs` (disjoint early-only `setA` keeps referenced branches as merge winners; asserts merged v2.1 file has `short==0`; phase-1 asserts flag-on → v2.0 referenced)
- [x] write test: full rebuild → squeeze cycle produces readable files (exercises the flag-toggle window, not just a standalone two-input merge) — `TestCommitmentRebuildSqueezeReadableAfterReload` via `RebuildCommitmentFiles(squeeze=true)`
- [x] run tests — must pass before next task

### Task 10: Version-aware commitment integrity checks

**Files:**
- Modify: `db/integrity/commitment_integrity.go` (`:514`, `:1266`, `:1864`, `:1961`)
- Modify: corresponding integrity test(s) if present

- [x] update the integrity logic so it expects references only in files that are actually
      referenced (version < v2.1 AND range ≥ threshold); a v2.1 file must not be flagged for
      lacking references — added `Version()` to the `kv.VisibleFile` interface, exported
      `state.CommitmentBranchReferenced`, and routed the three integrity decision sites
      (`checkCommitmentKvDeref` skip, `checkStateCorrespondenceBase`/`checkHashVerification`
      `isReferencing`) through `commitmentFileReferencing(file, stepSize)`
- [x] write/extend tests covering a mixed v2.0/v2.1 datadir passing integrity —
      `commitment_version_test.go::TestCommitmentFileReferencing` (decision predicate) +
      `commitment_version_integration_test.go::TestCheckStateVerify_VersionRegimes`
      (real v2.0-referenced and v2.1-plain ≥threshold merged datadirs each pass `CheckStateVerify`;
      the plain case also exercises the version-aware `CheckCommitmentKvDeref` skip)
- [x] run tests — must pass before next task

### Task 11: Correctness/integration coverage for the lazy transition

**Files:**
- Create/Modify: `db/state/...` integration-style test (mixed-version datadir)
- Modify: docs for the `erigondb-sync-integration-test-plan` scenarios

- [x] build a test datadir with both v2.0-referenced and v2.1-plain commitment files; assert reads are correct under flag on and flag off — `commitment_mixed_version_test.go::TestMixedVersionDatadirReadsConsistentAcrossFlag`: caps domain merges at a small frozen size (`SetErigondbDomainStepsInFrozenFile`) so a flag-on referenced `v2.0-commitment.0-4.kv` (holding a disjoint `setA` that is never superseded) coexists with later flag-off `v2.1` plain files; the recomputed root is identical with the flag on and off
- [x] assert flipping the flag both directions on a populated datadir stays correct (no upfront migration) — same test flips on→off→on; root stays equal and the on-disk file set + per-regime counts are unchanged (reads perform no migration, since they never `Commit`)
- [x] extend the 3 erigondb-sync scenarios (legacy / fresh+downloader / fresh+no-downloader) to assert `references_in_commitment_branches` resolves per scenario — resolution covered by `db/state/erigondb_settings_test.go::TestResolveErigonDBSettings{LegacyWritesTrue,FreshNoDownloaderWritesTrue,FreshWithDownloaderDoesNotWrite}`; documented the new field per scenario in the `erigondb-sync-integration-test-plan` skill (success-criteria table + per-scenario `cat` expectations)
- [x] run tests — must pass before next task

### Task 12: Verify acceptance criteria

- [x] verify every Overview requirement is implemented (file-driven flag, default true, version-aware read/merge, threshold stays const, no CLI flag) — file-driven flag (`ReferencesInCommitmentBranches` in `erigondb_settings.go`), default true (`config3.DefaultReferencesInCommitmentBranches`), version-aware read (`commitmentBranchReferenced` gates on `fileVersion.Less(version.V2_1)`), threshold const (`minStepsForReferencing = 2`), no CLI flag (no flag in `cmd/utils/flags.go` or elsewhere — pre-existing `experimental.concurrent-commitment`/`prune.include-commitment-history` are unrelated)
- [x] verify edge cases: absent field, explicit false/true, mixed-version datadir, flag flip both ways, downloaded-toml-not-rewritten — covered by `erigondb_settings_test.go` (`AbsentFieldNormalizesToTrue`+content-equal, `ExplicitFalseHonored`+content-equal, `ExplicitTrueHonored`, `LegacyWritesTrue`, `FreshNoDownloaderWritesTrue`, `FreshWithDownloaderDoesNotWrite`) and `commitment_mixed_version_test.go::TestMixedVersionDatadirReadsConsistentAcrossFlag` (flip on→off→on)
- [x] run full suite: `make test-all` (or `make test-short` for fast loop) — `make test-short` passed (exit 0, 0 FAIL); affected packages `db/state` (44s), `db/integrity`, `db/state/statecfg` green individually
- [x] `make lint` until clean (non-deterministic); `make erigon integration` — lint clean (0 issues); build exit 0
- [x] confirm no remaining references to `ReplaceKeysInValues` / `AggregatorSqueezeCommitmentValues` — repo-wide grep: only the plan doc itself references them; zero in code

### Task 13: [Final] Documentation

- [x] document the `references_in_commitment_branches` `erigondb.toml` field and the producer
      workflow (set `false` before running to publish plain snapshots) where erigondb settings
      are described — field documented per-scenario + new "Producer workflow" section in the
      `erigondb-sync-integration-test-plan` skill; field/regime + producer workflow summarized in
      `db/agents.md` ("Runtime settings (`snapshots/erigondb.toml`)")
- [x] update `CLAUDE.md` / component `agents.md` only if a new pattern warrants it — added a
      "Runtime settings (`snapshots/erigondb.toml`)" section to `db/agents.md` documenting the
      per-datadir settings pattern (resolved by `state.ResolveErigonDBSettings`, never rewrites a
      downloaded toml) and the three fields; no `CLAUDE.md` change needed (no new agent convention)
- [x] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — informational only.*

**Producing/publishing v2.1 snapshots:**
- To publish unreferenced snapshots: set `references_in_commitment_branches = false` in the
  producer datadir's `erigondb.toml` before running; merges produce v2.1 plain files; the toml
  ships with the snapshot set so consumers inherit `false`. This supersedes the `awskii/36exp`
  const-flip — that branch's `state_schema.go` const change and standalone `versions.yaml`/
  `version_schema_gen.go` bump are no longer needed once this lands.

**Downgrade caveat (accepted):**
- The bump is a minor (v2.0 → v2.1). A *downgraded* older binary shares major version 2 and may
  not hard-reject v2.1 files, so it could silently misread plain files as referenced. Verify
  behavior on the oldest supported release before advertising downgrade as safe; if unacceptable,
  revisit the major-bump option.

**Fleet rollout:**
- Confirm mainnet/default nodes (flag absent → true) keep writing byte-compatible v2.0 referenced
  files (no behavior change for them) before rolling the non-mainnet snap36 hosts onto plain v2.1.
