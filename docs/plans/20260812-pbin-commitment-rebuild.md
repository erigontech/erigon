# PBin commitment rebuild: a file-producing tool, code-zone coverage, per-code-hash chunking

## Overview

The rebuild is a **tool that produces commitment `.kv` files**. It re-derives them from the
account/storage/code domain files and must work regardless of what commitment scheme the datadir is
currently configured for: pointing it at a hex datadir and asking for bin output is the EIP-8347
offline migration case, not a misconfiguration. A node later started on those files passes
`--experimental.bin-commitment`; that is where scheme enforcement belongs, not in the producer.

Three things follow, and they are what this plan is mostly about:

1. The target variant is a **parameter of the rebuild**, not a property of the datadir or of process
   state. Today the shard loop reads `execctx.PickTrieVariant()`.
2. The rebuild **never rewrites the source datadir's commitment files**. It writes new files to a
   separate output location together with the `erigondb.toml` that describes them. This is a
   correctness requirement, not tidiness: nothing in a commitment `.kv` filename or header records
   the trie variant (`domainFiles` matches on `domain.String()` alone, `squeeze.go:1224-1237`), so
   bin files sitting in a hex datadir are indistinguishable from hex ones and would be read as hex.
   The variant lives only in `erigondb.toml` (`trie_variant`, `trie_hash` — both `omitempty`, absent
   meaning hex/keccak, `erigondb_settings.go:32-35`, `:50-61`), so the toml must travel with them.
3. Code is untested through the rebuild. Every existing bin rebuild fixture writes `EmptyCodeHash`
   and never populates `kv.CodeDomain`, so the whole `CODE_ZONE` is uncovered, and the gate that
   decides whether chunks are emitted fails **silently** when wrong.

Conversion from an existing hex commitment is not attempted: the hex commitment domain commits a
different key space under a different hash and nothing in it maps onto binary-tree keys.

## Context (from discovery)

Verified against `binary-trie` @ `2876b42430c`.

Already working:
- Bin rebuild end to end — `execution/commitment/backtester/pbin_m1a_test.go:307`
  (`TestPBinM1AForwardRunMatchesRebuildFromDomains`), incl. restart and collation/merge.
- Bin branch records survive merge byte-for-byte — `pbin_m1a_test.go:359`
  (`TestPBinM1ABranchRecordsSurviveCollationAndMerge`), through `agg.MergeLoop` (`:381`) and a
  folder reopen (`:385`).
- Range→shard→merge — `db/state/squeeze.go:876` (`RebuildCommitmentFiles`), `:1157`
  (`rebuildCommitmentShard`).
- **State reads are already files-only**: `commitmentdb.NewFilesOnlyStateReader(rwTx, …)`
  (`squeeze.go:1056`, defined `execution/commitment/commitmentdb/reader.go:141-170`) consults no
  history index, no live DB state, and no `.kv` past the boundary. Key iteration is likewise from
  files: `acRo.FileStream(...)` (`squeeze.go:999-1005`).

What blocks a scheme-independent rebuild:
- The shard loop takes the variant from process state — `squeeze.go:1022`,
  `trieVariant := execctx.PickTrieVariant()`, fed to `iterTrieCfg.Variant` (`:1049`).
- `reconcileTrieVariant` (`db/state/erigondb_settings.go:66-107`) refuses
  `--experimental.bin-commitment` on a hex datadir (`:96-98`: "the bin trie needs a fresh datadir")
  and force-enables the flag process-wide on a bin datadir (`:76-79`). It also binds the hash suite
  from the datadir's `trie_hash` (`:88-93`).
- **The way through is not to bypass it.** If the rebuild takes its target variant as an explicit
  parameter and never sets `statecfg.ExperimentalBinCommitment`, opening a hex datadir passes
  reconcile cleanly (hex + no bin flag is a legal combination), and the rebuild still constructs a
  bin engine internally. No bypass, no process-global mutation.

Residual DB dependencies — the rebuild is files-only for *state*, not fully DB-free:
- `a.db.BeginRo(ctx)` (`squeeze.go:961`) exists only to resolve txNum→blockNum via
  `txNumsReader.FindBlockNum` / `Last` (`:1008-1017`), which is recorded in the rebuilt commitment
  metadata.
- `rwDb.BeginTemporalRw(ctx)` (`squeeze.go:1041`) is the write path for the output files and the
  `SharedDomains` plumbing.
These are not state reads and do not compromise the files-only property, but the plan should not
claim the tool needs no database.

The code gaps:
- **Code writes are dropped during rebuild**: `squeeze.go:1161` — `sd.DiscardWrites(kv.CodeDomain)`.
  A fixture must land code in the domain **files** before the rebuild runs.
- **The chunk-emission gate is `CodeSize`, not `Code`**: `pbin_update_stream.go:219` — `chunkSource`
  returns `nil, {}, nil` when `update.CodeSize == 0`, before reading code. `CodeSize` is populated
  only when `sdc.readCodeSize` is set (`commitmentdb/commitment_context.go:1107, 1119-1121`),
  derived from `sdc.variant == commitment.VariantBinPatriciaTrie` (`:283`, `:767`, `:815`). Wrong
  gate ⇒ **silently empty code zone and a wrong root, no error**. Under hex the same read is skipped
  entirely, so this cost is bin-only.
- **Chunking is per account, not per code hash**: `pbin_update_stream.go:233` (`queueChunks`) appends
  every chunk for every account; dedup happens only at flush (`:253-263`), after sorting. For N
  accounts sharing a code hash that is N chunkifications and an `O(N·C log N·C)` sort to emit C
  leaves. Mainnet duplication is ~65%.

What dedup does **not** save:
- `TrieContext.Account` reads `kv.CodeDomain` and computes `crypto.Keccak256Hash(code)` per
  code-bearing account under bin (`commitment_context.go:1107-1120`). The code domain is keyed by
  **address**, so N accounts sharing bytecode hold N copies and that read/verify is genuinely per
  account; deduping it by hash would drop a real integrity check.
- Saving is N→1 chunkifications and `O(N·C log N·C)` → `O(C log C)` at flush.
- Adjacent, **not in this plan**: `chunkSource` calls `codeOf(plainKey)` (`:222`), re-reading the
  *same address* `Account` already read — a redundant read per code-bearing account, removable by
  threading the bytes through rather than by hash dedup. Wants its own measurement.

Settled, not in scope:
- **Squeeze is never made to work under bin**; `--squeeze` for a bin rebuild errors (Task 5).
- **The with-history path is out of scope.** `RebuildCommitmentFilesWithHistory` (`squeeze.go:490`)
  is taken when history is enabled (`cmd/integration/commands/commitment.go:337-353`); it leaves
  `EnableTrieWarmup` true (`:541`) and calls `EnableParaTrieDB` ungated (`:551`, `:657`). Both are
  currently inert for bin (`Process` discards `WarmupConfig`, `pbin_patricia_hashed.go:152`;
  `EnableParaTrieDB` early-returns on an empty pending variant, `commitment_context.go:101-103`).
  A bin rebuild is expected to run `--no-history`; that is an **unenforced convention**
  (`commitment.go:328-331` auto-sets it only for `resume && !commitmentHistoryEnabled`).

## Development Approach

- **Testing approach**: TDD for Tasks 1-4, where the claim under test *is* the deliverable.
- complete each task fully before the next; **all tests pass before moving on**
- tests are separate checklist items
- commit locally only, do not push

## Testing Strategy

- in-repo tests only; no UI surface, so no e2e
- **Acceptance oracle.** A rebuild is state-derived: it reads each account once at the range's final
  state (`FilesOnlyStateReader(rwTx, lastTxnumInShard-1)`, `squeeze.go:1056`) and emits chunks for
  that account's final code. A forward run accumulates chunks that are never removed
  (`pbin_update_stream.go:167-172`), so over history with destroyed-and-redeployed contracts the two
  legitimately diverge. Therefore:
  - the oracle is **forward-run equality on a chain with no account destruction**, and
  - for a chain that does destroy accounts, the assertion is against a **freshly derived tree over
    the same final state**, not the forward root.
  This is the correct target for EIP-8347 migration, where the state-derived tree is the answer.

## Implementation Steps

### Task 1: Make the target variant a rebuild parameter (TDD)

**Files:**
- Modify: `db/state/squeeze.go`
- Modify: `cmd/integration/commands/commitment.go`
- Create: `db/state/rebuild_variant_test.go`

- [x] write the test first: a rebuild asked for bin output on a **hex-configured datadir** produces
      bin commitment files, without setting `statecfg.ExperimentalBinCommitment` and without
      tripping `reconcileTrieVariant` (`erigondb_settings.go:96-98`)
- [x] add an explicit target variant parameter to `RebuildCommitmentFiles`, replacing the
      `execctx.PickTrieVariant()` read at `squeeze.go:1022`; default to the picked variant when the
      caller does not specify, so existing callers are unchanged
- [x] the rebuild MUST NOT mutate process-global `statecfg`; assert that in the test
- [x] for a bin target, bind the hash suite explicitly for the rebuild rather than relying on the
      datadir's `trie_hash` (`erigondb_settings.go:88-93`), and record which suite was used
- [x] keep the `EnableParaTrieDB` gate (`squeeze.go:1057-1058`) keyed on the hex parallel/streaming
      variants, driven by the target variant
- [x] write a test that the hex rebuild path is unchanged when no target is specified
- [x] run tests — must pass before Task 2

### Task 2: Write output to a separate location, self-described

**Files:**
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `cmd/integration/commands/flags.go`
- Create: `cmd/integration/commands/commitment_output_test.go`

The source datadir is a read-only input. Its existing commitment files MUST survive untouched, and
the new files MUST NOT land beside them: nothing distinguishes a bin `commitment.N-M.kv` from a hex
one by name, location or header (`domainFiles`, `squeeze.go:1224-1237`), so co-locating them means
the wrong set gets read as the datadir's commitment.

- [x] add an output-directory flag; refuse to run without one for a bin target, rather than
      defaulting to the source datadir
- [x] stage the output as a datadir-shaped directory whose `SnapDomain` holds **hardlinks** to the
      source's account/storage/code `.kv` files and **no** commitment files, so the rebuild reads the
      same inputs without copying them and writes its commitment files into the staging dir
- [x] fail if the output `SnapDomain` already contains commitment files, unless `--resume` was asked
      for; never overwrite silently
- [x] write `erigondb.toml` into the output directory with `trie_variant` and `trie_hash` set to what
      was actually produced, so a node started on it with `--experimental.bin-commitment` passes
      `reconcileTrieVariant` rather than being refused at `erigondb_settings.go:96-98`
- [x] carry `step_size` and `steps_in_frozen_file` over from the source settings, so the output
      describes the same geometry the files were built at
- [x] write tests: the source datadir's commitment files are byte-identical before and after; the
      output holds the new files plus a toml describing bin and the hash used; a second run without
      `--resume` refuses instead of overwriting
- [x] run tests — must pass before Task 3

Landed shape: staging writes the toml once, naming the target before the rebuild starts. The tool
repoints its datadir at the output and reopens it, so `reconcileTrieVariant` runs over the staged
directory: a toml that omits the variant reads as hex and the bin run is refused there, before any
work. Naming it up front also leaves an interrupted run self-describing rather than passing its bin
files off as hex. Applying the variant process-wide is a no-op — only `--experimental.bin-commitment`
makes the target bin in the first place. A bin target also pins `references_in_commitment_branches =
false`, which both satisfies reconcile's bin rule and keeps the refs-driven squeeze pass
(`squeeze.go`, the `wantsReferencesInBranches` gate) out of a bin run. `--output.datadir` is refused
together with `--reset` and `--clear-commitment`, which write to the source, and with a history
rebuild, which the staged directory holds no files for.

The same "a `.kv` names no variant" argument runs the other way, and the flags alone cannot tell:
the target is resolved before the datadir is opened, so a run without `--experimental.bin-commitment`
takes a bin datadir for a hex one and writes hex files into it. An in-place run therefore reads the
source's `erigondb.toml` and refuses a target the datadir does not record. A `--resume` run compares
the same way against the output's own toml, which staging is about to overwrite.

### Task 3: Prove the code zone through a rebuild (TDD)

**Files:**
- Create: `execution/commitment/backtester/pbin_rebuild_code_test.go`

- [x] write the test first, modelled on `pbin_m1a_test.go:307`, with a fixture that writes real code
      into `kv.CodeDomain` and flushes it to domain **files** before the rebuild (`squeeze.go:1161`
      discards code writes made during the rebuild)
- [x] case: contract whose code spans more than one code group; rebuilt root == forward root
- [x] case: two accounts sharing byte-identical code; root matches, shared chunks emitted once
- [x] case: contract whose code contains an all-zero chunk; that chunk absent from the tree
- [x] case: EIP-7702 delegated account; `DELEGATION` leaf present, no code leaves
- [x] **case: accounts sharing code split across two shards of the same range**, so the shard
      boundary (`squeeze.go:1035`) falls between them; this is the case Task 4 puts at risk
- [x] assert the rebuilt code zone is non-empty, so the test cannot pass by emitting nothing
- [x] run tests — must pass before Task 4

Landed shape: the code zone is read back through the commitment branch records, filtered to the
paths that open with the CODE zone byte. A chunk leaf and a delegation indicator are values no state
domain holds, so the record carries them inline and a byte scan answers presence without decoding.
The zero-chunk case pins absence by record count against a two-chunk and a three-chunk control,
since the count follows the leaf count. Mutation-checked: dropping `queueChunks` fails all four
code-zone cases, dropping the delegation branch fails the delegation case, and a seen-set of code
hashes that survives across `Process` calls fails the multi-shard case — the hazard Task 4 must
avoid.

Two shards need a range wider than `commitment.DefaultRebuildShardMaxSteps`, which the fixture
reaches with one txNum per step and a 128-step merged accounts file. That range must also hold more
keys than steps: `keysPerStep` is an integer division at `squeeze.go` in the shard loop, and a
sparser range divides by zero.

### Task 4: Make the CodeSize gate loud, then chunk once per code hash (TDD)

**Files:**
- Modify: `execution/commitment/pbin_update_stream.go`
- Create: `execution/commitment/pbin_codesize_gate_test.go`
- Create: `execution/commitment/pbin_chunk_dedup_test.go`

Ordering is part of the deliverable — do not reorder:

- [x] write the gate test first: an account whose `CodeHash` is not the empty-code hash but whose
      `CodeSize` is 0 must error, not yield an empty code zone
- [x] add that check in `chunkSource` before the `CodeSize == 0` short-circuit (`:219`)
- [x] test that a bin context has `readCodeSize` set, pinning `commitment_context.go:283`
- [x] add a seen-set of code hashes to `pbinUpdateStream` recording code size per hash, cleared in
      **`reset()`** (`pbin_update_stream.go:85`), *not* `release()`. `release()` runs only on
      `Release()` (`pbin_patricia_hashed.go:120-127`) while `reset()` runs per `Process` (`:104`);
      a cache cleared only on release survives many `Process` calls on a pooled engine, and after an
      unwind rolls the tree back (`commitment_context.go:299-303`) a stale hit skips chunks the tree
      no longer holds
- [x] **disable the dedup when `s.witnessPass` is set.** `chunkSource:216-218` returns
      `keccak.Sum256(override)` under a witness pass, not `update.CodeHash`, so a set keyed on
      `update.CodeHash` keys on the wrong value; and `Witnesses` collects every emitted treeKey into
      `provedKeys` (`pbin_witness.go:104-107`), so deduping shrinks the witness regardless of key
- [x] place the hit check **after** the gate check, so a `CodeHash != empty && CodeSize == 0` account
      errors on every occurrence, not only the first
- [x] on a hit skip `codeOf` and `queueChunks`, but still emit this account's `CODE_HASH` leaf and
      the `DELEGATION` removal — per account, no code bytes needed
- [x] on a hit verify `update.CodeSize` matches the recorded size and error on mismatch; this
      replaces both the length check at `chunkSource:226-229` and the cross-account conflict check
      at `flushCodeChunks:258-262`
- [x] **exclude delegation indicators from the set.** The skip path emits the non-delegation sibling
      pair (`:144-149`) while a delegation takes the early-return branch (`:137-143`); excluding
      them is what makes *hit ⇒ non-delegation* true, and that invariant is the entire licence for
      skipping `chunkSource`. Not because hits are rare — a designator is `0xef0100 || target`
      (`pbin_values.go:81-83`), so mass delegation to one implementation makes them identical
- [x] tests: N accounts sharing code chunk exactly once; root unchanged vs pre-dedup; size mismatch
      on a shared hash errors; delegation accounts unaffected; witness `provedKeys` unchanged;
      flush sort input shrinks from N·C to C
- [x] run tests — must pass before Task 5

Landed shape: the gate lives inside the `CodeSize == 0` branch, so a codeless account still
short-circuits and only a hash with no size errors. A cache hit returns nil code with the account's
hash, which routes through the existing non-delegation branch and chunkifies nothing — no second
skip path to keep in step with the first. `flushCodeChunks` keeps its two-values check: the witness
pass runs with the cache off, so that is still the only guard there.

The gate is loud enough to reject fixtures, not just wrong contexts: a corpus account carrying an
invented code hash with no code is an unreachable state, and ~30 of them across the pbin tests now
say `empty.CodeHash`. That keeps every leaf key and leaf count where it was — only the CODE_HASH
leaf value moves — so the oracles follow without restructuring.

Mutation-checked: dropping the cache lookup, moving the clear from `reset()` to `release()`,
removing the gate, caching delegation indicators, and letting a witness pass touch the cache each
fail a test. The last one is only visible as the cache being non-empty after a witness pass — chunk
keys are deduped again at flush, so the emitted key set alone cannot tell the two apart.

### Task 5: Reject `--squeeze` for a bin rebuild

**Files:**
- Modify: `cmd/integration/commands/commitment.go`
- Create: `cmd/integration/commands/commitment_squeeze_reject_test.go`

- [x] return an error when `squeeze` is set and the **requested rebuild target** is bin, in
      `commitmentRebuild` alongside the existing mutual-exclusion checks (`commitment.go:288-293`).
      Put it there, **not** at `squeeze.go:1125`: squeeze is the final pass, so erroring there wastes
      the entire rebuild, while `commitmentRebuild` runs before any work and `squeeze` is not
      consumed until `:403`/`:407`
- [x] key the check on the requested target variant, not on the datadir's scheme — the whole point of
      Task 1 is that those can differ
- [x] error text names the reason: bin branch payloads are not `BranchData`
      (`execution/commitment/pbin_branch.go:55-57`) and their field bits collide (`:33-38`)
- [x] tests: bin target + `--squeeze` errors; bin target without it proceeds; hex unchanged
- [x] run tests — must pass before Task 6

Landed shape: `refuseSqueezeForBinTarget` is a predicate over `(target, squeeze)` — no globals, no
datadir read — called from `commitmentRebuild` next to the other flag conflicts. The bits do collide
concretely: `pbinFieldBranch` is 2 where hex reads `fieldAccountAddr`, and `pbinFieldAccountAddr` is
4 where hex reads `fieldStorageAddr`, so a squeeze pass over a bin record would take a branch flag
for a plain-key flag and rewrite bytes that are not keys.

Ordering is pinned by passing a **nil** db to `commitmentRebuild`: the check must fire before
anything touches the database or the filesystem. Mutation-checked — dropping the call makes that test
crash in `blocksIO`. The two "proceeds" cases are asserted on the predicate rather than through
`commitmentRebuild`, because getting past the checks with a nil db reaches `blocksIO`, whose
`sync.Once` singleton would then be poisoned for every other test in the package.

### Task 6: Report commitment size per range

**Files:**
- Modify: `db/state/squeeze.go`
- Modify: `cmd/integration/commands/commitment.go`
- Create: `cmd/integration/commands/commitment_report_test.go`

- [x] stat the commitment `.kv` files in `dirs.SnapDomain` after the rebuild; report bytes per range
      and a total
- [x] for key counts, return them from `RebuildCommitmentFiles` — today it returns `latestRoot []byte`
      only (`squeeze.go:876`) and the counts exist solely as `logger.Info` output (`:1104-1105`,
      `:1198-1200`), which a caller cannot read. Do not scrape logs
- [x] report unique code hashes vs total code-bearing accounts **per shard**, the ratio Task 4 acts on
- [x] stable field names so the output pastes into a table
- [x] write a test over a temp dir asserting reported sizes match the files on disk
- [x] run tests — must pass before Task 7

Landed shape: `RebuildCommitmentFiles` returns a `*RebuildReport` — one entry per rebuilt range, each
holding its shards. The code counts come from the bin engine, which counts what its update stream
chunkified per `Process` and clears that with the chunk cache in `reset()`; the hex engines expose no
such method and report zeros. Every range walks the whole key set at its own boundary, so the counts
are per shard and summing them across ranges counts the same account again — the ratio to read is
within one shard.

Sizes stay on the caller's side: the rebuild does not know which directory it wrote to once
`--output.datadir` repoints `datadirCli`, and a run with an output datadir must size the staged
directory rather than the source it read. The report is three tab-separated tables under fixed column
names, with a `total` row over the files.

The shard count no longer has to be scraped from log lines: the across-shards test reads
`len(report.Ranges[0].Shards)` and the log-handler counter it used is gone. Mutation-checked: dropping
the `.kv` filter, sorting the files by name rather than by step, sizing the source instead of the
output, dropping the per-shard append, and clearing the counts in `release()` rather than `reset()`
each fail a test.

### Task 7: [Final] Verify acceptance criteria

- [x] a bin rebuild runs against a hex-configured datadir and produces usable bin commitment files
      — `TestRebuildCommitmentFilesBinTargetOnHexDatadir`, which restores the root back out of the
      files it wrote rather than trusting the return value
- [x] the source datadir is unmodified: its commitment files and `erigondb.toml` are byte-identical
      before and after — `TestStageRebuildOutputLeavesSourceIntact` compares a content map of the
      whole `Snap` tree, which is where `erigondb.toml` lives
- [x] the output directory alone is sufficient to start a node with `--experimental.bin-commitment`
      — `TestRebuildOutputStartsUnderTheBinFlag`
- [x] the rebuild leaves process-global `statecfg` unmodified —
      `rebuildVariantProcessStateUntouched`, `TestStageRebuildOutputLeavesProcessConfigUnmodified`
- [x] a bin rebuild with code-bearing accounts reproduces the forward root (Task 3 oracle)
      — the four `TestPBinRebuildCode*` / `TestPBinRebuild*Code*` cases
- [x] the multi-shard shared-code case passes, proving the seen-set is correctly scoped
      — `TestPBinRebuildSharedCodeAcrossShards`
- [x] the hex rebuild path is unchanged — `TestRebuildCommitmentFilesDefaultTargetIsProcessVariant`
- [x] `--squeeze` with a bin target errors before any rebuild work starts
      — `TestCommitmentRebuildRefusesSqueezeBeforeAnyWork`, ordered by a nil db
- [x] `--resume` still skips covered ranges for a bin target
      — `TestRebuildCommitmentFilesBinTargetResumeSkipsCoveredRanges`
- [x] chunkification count and flush sort size are proportional to unique code hashes, not accounts
      (code-domain reads remain per account by design — see Context)
      — `TestPBinSharedCodeIsChunkedOncePerHash` pins reads, queued chunks and the shard counts
- [x] witness output is byte-identical to pre-dedup — `TestPBinWitnessPassChunksEveryAccount`
- [x] run: `go test ./db/state/... ./execution/commitment/... ./cmd/integration/...`
- [x] run `go build ./...` and `gofmt -l` over touched packages

Landed shape: this pass was verification, not implementation — nine of the eleven criteria were
already pinned by a Task 1-6 test, so the work was mapping each one to the assertion that would fail
if it regressed, and writing the two that nothing covered.

The first gap was the startability of the output. Task 2 asserted what `finalize()` puts in the toml,
which is not the same claim as the settings resolver accepting that directory: the check that would
refuse it lives in `reconcileTrieVariant`, and nothing ran it. The new test sets the bin flag and
hash, resolves the output, and resolves the source for contrast — the source must be refused, since
being refused there is the reason a separate output directory exists at all.

The second was resume under a bin target. The skip is a comparison against the commitment txNum in
files, so it never consults the scheme, but "it does not consult the scheme" is exactly the kind of
claim that a later change to the loop breaks silently. The test rebuilds twice over one datadir and
asserts the second run reports no ranges and leaves the commitment files byte-identical.

Both were mutation-checked, since a verification test that passes on first run has proven nothing
yet: disabling the range skip fails the resume test, and having `finalize()` omit the variant fails
the startability test with the refusal it exists to prevent.

## Post-Completion

*No checkboxes — these need a real datadir.*

**Measurement run:**
- bin rebuild against a mainnet-scale **hex** datadir — the migration case — recording commitment
  bytes per range
- hex rebuild against the same datadir as the comparison arm
- report **within-shard** unique-vs-total code hashes, not just the global ~65%: dedup is per
  `Process`, so the global figure is an upper bound the task does not deliver
- provenance: machine, datadir, step size, ranges, hash suite

**EIP-8347 follow-on:** offline re-derivation only. Snapshot distribution, dual-check verification
and BAL replay from the anchor are separate and out of scope.
