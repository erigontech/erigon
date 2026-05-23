# Fork identification — implementation plan

## Context

`erigon-documents → ethereum/design/erigon-archive/fork-spec.md` is the design
(Identification, Directory layout, Cut-point placement — revised 2026-05-22).
This plan covers the **erigon-side implementation**, and only the
*identification* layer.

Explicitly **not** in this plan: the `erigon fork` CLI command, the `[parent]`
section plumbing, the per-stream directory layout, and the cut/patch-file
mechanics. Those are later phases — the `[parent]` read path depends on sparse
reads (`#20587`) and the fork command is a larger surface. Identification is
separable and ships first because it de-risks the manifest format and is
verifiable on chains that exist today.

## Phase 1 — manifest identity + compatibility early-reject (the initial implementation)

The grounded, no-new-dependency slice. Every chain that exists today is a single
continuous lineage, so Phase 1 ships on root chains without the `[parent]`
machinery: the identity fields are populated, the consumer early-reject runs,
and same-chain peers pass it as a no-op.

### 1a. Schema (`db/downloader/chaintoml_v2.go`)

- Add to `ChainTomlV2`:
  - `GenesisFork string` — toml `genesis-fork`, `CRC32(genesis_hash)` hex.
  - `Forks []ForkActivation` — toml `[[forks]]`.
- New `ForkActivation`: `Name string`, `Block uint64` (toml `block`,omitempty),
  `Time uint64` (toml `time`,omitempty). Height forks set `Block`; time forks
  set `Time`.
- The live EIP-2124 fork ID is **derived** from `GenesisFork` + `Forks`; it is
  not a stored field.

### 1b. Publisher population (`GenerateV2` / the rolling publisher)

- Populate `GenesisFork` + `Forks` from the chain config.
- `GenesisFork`: the `p2p/forkid` checksum over the genesis hash with zero forks
  applied.
- `Forks`: `forkid.GatherForks(chainConfig, genesisTime)` returns the height +
  time fork lists; map each activation to a `ForkActivation`.
- The V2 generator currently has only `cfg.ChainName`; wire the resolved
  `*chain.Config` (+ genesis hash + genesis time) through to `GenerateV2`.

### 1c. Consumer derive + early-reject (`node/components/manifest_exchange/`)

- On a received peer manifest, parse `genesis-fork` + `[[forks]]` and derive the
  peer's EIP-2124 fork ID (`forkid.NewIDFromForks`).
- Build the local filter once at startup from the node's own chain config
  (`forkid.NewFilterFromForks`). Apply the EIP-2124 compatibility relation
  (fork-spec § *Compatibility, not equality*) — FORK_HASH lineage; the
  FORK_NEXT-sensitive rejections of the standard devp2p filter are relaxed,
  since snapshot manifests index immutable history.
- Incompatible → reject the manifest **before** `ValidateAdvertisement` / the
  quorum view ever sees its entries. Compatible → proceed unchanged.
- Plug-in point: the manifest-receive path in `manifest_exchange/provider.go`,
  as the first (cheapest) gate, ahead of the UCAN chain.

### 1d. Tests

- Schema round-trip: marshal/parse `genesis-fork` + `[[forks]]`; absent fields
  tolerated (V2 unknown-key tolerance).
- `GenesisFork` computation equals `forkid` for a zero-fork chain.
- Derived fork ID equals `forkid.NewID` for a populated chain config.
- Early-reject: a manifest with an incompatible derived fork ID is rejected; a
  compatible one is accepted; a same-chain peer at a different fork-schedule
  position (older release) is accepted.
- A `p2p_integration` scenario: a seeder advertising an incompatible derived
  fork ID is rejected by a consumer; a compatible seeder is accepted.

## Phase 2 — Full fork mechanism (no-hardlink architecture, converged 2026-05-23)

Replaces the earlier hardlink-based architecture. Open-question round closed
2026-05-23 (see `[[fork-identification-design-pickup-2026-05-23]]`).

### Converged decisions (do not re-litigate)

1. **No hardlinks.** Fragile across backup/migration/non-POSIX tools, races
   on retire/merge, no cross-FS support. Each erigon process runs ONE chain
   config and retires into its own snap dir; sharing happens at the swarm
   layer, not the filesystem layer.
2. **No multi-stream-on-disk.** Each datadir hosts ONE lineage. Multi-lineage
   hosting is out of scope for v1.
3. **No in-process fork command.** A fork is created by writing a derived
   `chain.Config` and starting erigon on a FRESH datadir. The process never
   pivots identity at runtime. An offline `erigon snapshots fork-from`
   utility produces a clean fork datadir from a parent's datadir (Phase 2c
   below).
4. **`chain.Config` IS the fork pin** — three new fields: `Parent`,
   `CutBlock`, `ParentManifestHash`. A non-empty `Parent` makes the config a
   fork; the three together define the cut deterministically. Today's
   `execution/chain/chain_config.go` has only fork-activation
   timestamps/blocks; these new fields are additive.
5. **Fresh-datadir mandate.** Derived config on a fresh datadir bootstraps
   pre-cut from the parent's swarm. On a datadir that already has post-cut
   parent data, erigon refuses to start at config-load time, pointing at the
   `snapshots fork-from` utility. No runtime adapt-policy flag — refusal is
   static.
6. **Straddle step = full self-contained.** Fork's first retire of the
   straddle step produces a normal full step-S file with the fork's lineage.
   The sub-step pre-cut sliver is duplicated locally; cheap. Patch form
   (split-step reads where pre-cut bytes resolve via the parent's file)
   deferred — folded into the sparse-read enhancement (Phase 4).
7. **Jagged-step naming.** A partial-coverage file encodes its boundaries in
   the filename, not via a `partial=true` schema flag. Full files always
   have step-rounded coordinates; jagged files have exactly one non-rounded
   coordinate. Parser disambiguation is intrinsic; no collision is possible.

   | Case | Example | Jagged at |
   |---|---|---|
   | Full step | `domain/v2.0-accounts.0-2048.kv` | nowhere (rounded both ends) |
   | Fork straddle (future patch form) | `domain/v2.0-accounts.985192321-2048.kv` | start (cut-txnum) |
   | Unwind partial | `domain/v2.0-accounts.2048-985192321.kv` | end (unwind-txnum) |
   | Block fork patch (future) | `v1.1-010895816-010896.seg` | start (cut-block) |
   | Block unwind | `v1.1-010895-010895816.seg` | end (unwind-block) |

   `PopulateFromName` tolerates non-rounded boundaries — populates
   `FromBlock`/`ToBlock` (or `FromTxNum`/`ToTxNum`) literally. Range-coverage
   queries handle jagged intervals naturally — they are just narrower than
   the rounded equivalents.

8. **Partial files also for unwinds.** The jagged-step mechanism does double
   duty. A snapshot unwind produces a `[step_start, unwind_txnum)` partial
   from a previously-full step. **An unwind partial is not advertizable
   until replaced** — either re-retired as a full file (post-unwind bytes
   correct + range re-rounded) or settled as a final partial (chain stays
   in unwound state). The publisher's L3 advertisement excludes the
   transient partial; the canonical layer effectively demotes it. This is
   the lifecycle counterpart to the canonical-layer demotion mechanism in
   the canonical-reorg work.

9. **Operator policy table — collapsed surface** (decisions 1-4 of the
   open-questions round resolved C and D out of the runtime flag set):

   | Scenario | Mechanism | Default |
   |---|---|---|
   | A: derived-chain bootstrap (download pre-cut) | `--fork.bootstrap-policy={auto, prompt, stop}` | `auto` |
   | B: adoption within fork's lineage on minority | reuse `AdoptionPolicy` | `auto`/`redownload` (same as non-fork — a fork is consensus-equivalent post-creation) |
   | D: derived config + datadir has post-cut parent data | static refusal at startup with hint | refuse + point at `snapshots fork-from` (no flag) |
   | E: extract-from-parent utility | separate `erigon snapshots fork-from` CLI | n/a (not a runtime policy) |

   Scenario C (parent manifest advances upstream) is **dropped** for v1 —
   fork pins to `ParentManifestHash` forever. Re-merge bytes-equivalence
   means we do not need to track parent's reshaping.

10. **`Provider.Restart()` is a formal API** (not v1-hidden). Same
    `*Inventory` pointer (drain + repopulate in place); emits `RestartBegin`
    on the bus; external components (Aggregator, BlockReader) subscribe and
    quiesce themselves (close OpenFolder readers); Restart drains + rescans;
    emits `RestartEnd`; components reopen. Callers = orchestrator
    (adoption-cutover) and provider (bootstrap-complete). Existing
    ChangeSet subscriptions on the preserved pointer continue working
    without reconnection.

### Sub-phases

**Phase 2a — `chain.Config` fields + `ChainTomlV2` `[parent]` section**
- `execution/chain/chain_config.go`: add `Parent string`, `CutBlock uint64`,
  `ParentManifestHash [20]byte`. JSON-marshalled with omitempty so existing
  configs are unaffected.
- `db/downloader/chaintoml_v2.go`: add a `[parent]` section to `ChainTomlV2`,
  mirroring the three chain.Config fields plus `CutTxNum uint64`,
  `CutBlockHash [32]byte`, `Name string` for human display.
- **No** `partial=true` / `partial-parent-hash` / `partial-parent-range`
  fields — the jagged filename carries the partiality and (when needed
  later) the parent-cascade pointer is name-derivable, not a schema field.
- Parser layer (`PopulateFromName`) tolerates non-rounded `FromTxNum`/
  `ToTxNum` and `FromBlock`/`ToBlock` (decision 7).
- Unit tests pin: round-trip with `[parent]` populated; jagged-name parses.

**Phase 2b — `Provider.Restart()` API**
- `node/components/storage/provider.go`: implement `Restart(ctx,
  RestartOpts) error` per the contract in decision 10.
- New flow events: `RestartBegin{Reason}` and `RestartEnd{}`. Add them to
  `node/components/storage/flow/events.go` next to the existing readiness
  events.
- Update `db/state/aggregator.go` + `db/snapshotsync/freezeblocks` (and any
  other Inventory consumers identified during impl) to subscribe to
  `RestartBegin` / `RestartEnd` and close/reopen their folder readers in
  response. Document the subscriber contract.
- Adoption-cutover (existing canonical-layer code) becomes a caller of
  `Restart()`; remove the inline orchestrator-level shutdown/rescan logic.
- Tests: in-memory orchestrator test that `Restart()` preserves the
  Inventory pointer, ChangeSet subscribers see no disconnection, and
  RestartBegin/End fire in the expected order.

**Phase 2c — `erigon snapshots fork-from` extract utility (offline)**
- CLI: `erigon snapshots fork-from --parent-datadir <path>
  --new-chain-name <name> --cut-block <N> [--cut-step-form=full]
  --new-datadir <path>`. Default form: `full`.
- Reads the parent's snap dir + chain config + manifest. Constructs the
  derived chain config (parent's config + `Parent`/`CutBlock`/
  `ParentManifestHash` fields populated). Snaps the cut to the step
  boundary covering `cut-block` by default; an intra-step cut produces a
  jagged file (decision 7).
- Writes the new datadir with:
  - Derived `chain.toml` + `chain.Config`
  - Pre-cut snap files copied (decision 1: no hardlink)
  - The straddle-step file in full form (decision 6) — extracted by reading
    the parent's straddle-step content + truncating to the cut boundary
- The fork process is then started by the operator on the new datadir;
  Phase 2e runs at startup to detect the fork config.
- Live test: produce a sepolia fork at a recent step boundary; verify the
  new datadir starts cleanly and runs.

**Phase 2d — Fork-follower bootstrap (fresh datadir + parent swarm fetch)**
- A consumer node configured with a derived chain config that does NOT have
  the parent's snap files locally. The startup path:
  1. Detects `chain.Config.Parent != ""` → fork mode.
  2. Fetches the parent manifest by `ParentManifestHash` from the
     manifest-exchange flow. Aborts if hash does not match (catches
     tampered or wrong-version derived configs).
  3. Classifies the union of (parent's pre-cut files, fork's files):
     - Pre-cut files → download into the snap dir's normal layout
       (`domain/`, `history/`, `idx/`, block-segs root).
     - Fork-divergent files (post-cut + straddle) → also download into the
       normal layout (decision 2: no fork-tag subdir).
  4. Once all are local + indexed, `InitialDownloadsComplete` + the
     post-Indexed gate fire as for non-fork bootstraps. `Provider.Restart()`
     is NOT involved at first-bootstrap (it is only for later cutover
     events).
- Inventory's by-name lookup is sufficient — no by-hash cascade index
  needed for v1 (the patch-form cascade is folded into Phase 4).
- Tests: a derived-chain consumer + a regular-chain seeder + a fork
  publisher. Consumer downloads parent's pre-cut + fork's post-cut +
  straddle from the right peers.

**Phase 2e — Datadir-conflict refusal at startup**
- During `chain.Config` load, if `Parent != ""` AND the datadir already
  contains snap files past `CutBlock` from the parent's lineage, refuse to
  start. Error message points at `snapshots fork-from`. Static check; no
  runtime adapt policy.
- Tests: derived config + clean datadir → starts. Derived config + parent
  data past cut → fails with the expected hint.

**Phase 2f — Jagged-step file support + advertising-lifecycle rule**
- Inventory + validation layer accept jagged-step files (extension of 2a's
  parser change). The publisher's L3 advertisement filter excludes
  unwind-partials per the advertising-lifecycle rule (decision 8): a file
  whose name carries a jagged-end coordinate is advertizable only if its
  lifecycle state is `LifecycleAdvertisable` AND it is NOT in the
  "pending replacement" set.
- Add `Inventory.PendingReplacement([]string)` API so the canonical-reorg
  work + the fork-creation code can mark unwind/cut transients.
- Tests: unit tests for the filter + scenario coverage in
  `node/components/integration/snapshot/scenarios/` for an unwind that
  produces a partial, advertises only after re-retire, and a fresh follower
  that sees only the post-replacement file.

**Phase 2g — Tests + sepolia validation**
- Unit: round-trip schema with `[parent]`; `Restart()` contract;
  fork-bootstrap classification; jagged-name parser; datadir-conflict
  refusal; advertising-lifecycle filter.
- Harness scenarios (integration tests):
  - Fork creation via `snapshots fork-from` from a sepolia parent dir.
  - Fork process retire + publish (full straddle step).
  - Fork-follower fresh-bootstrap.
  - Datadir conflict refusal.
  - Unwind producing a jagged file, replacement re-rounding it.
- Live: end-to-end sepolia fork — create from a recent step boundary,
  observe retire + publish; follow the fork from a fresh process; soak.

## Phase 3 — Trust across the cut

The Authority/Content two-UCAN model spanning a fork's `[parent]`. A fork's
publishers root their post-cut data on their own Authority UCAN, independent
of the parent's; the consumer's two-phase verification (parent-portion vs
fork-portion against each side's trust anchor — fork-spec § *Trust model*) is
specified concretely. Independent of Phase 2's mechanics.

## Phase 4 — Sparse-read enhancement + patch-form straddle

Folds two things that were independent in the prior plan but become the
same code path under sparse:

- When `#20587` lands, a `CascadeReader` abstraction can resolve pre-cut
  byte ranges from the swarm instead of from a local copy. The
  fork-follower's pre-cut download volume drops from full-copy to sparse.
- The patch-form straddle (where the fork's straddle file is
  `[cut-txnum, step_end)` and the pre-cut sliver resolves via the parent
  step-S) becomes a natural consequence of sparse — the cascade just
  fetches the missing bytes from the parent's file via sparse.

This consolidates the previously-separate Phase-2d cascade work into Phase
4. No read-path or schema changes from Phase 2's perspective — Phase 2
ships with full straddle files; Phase 4 introduces the optimised forms.

## Sequencing

Phase 1 ships first (DONE, commits `09bad5abff`, `0355534fc0`, `2436972dd8`).

Phase 2 sub-phases:
- 2a (chain.Config fields + V2 `[parent]` schema) ships first — pure schema,
  no behaviour change, unblocks everything else.
- 2b (`Provider.Restart()`) is independent and lands in parallel; it is
  also useful outside the fork story (adoption-cutover).
- 2c (offline `snapshots fork-from` utility) depends on 2a + the V2
  manifest reader. Independent of 2b.
- 2d (fork-follower bootstrap) depends on 2a; parallel with 2c.
- 2e (datadir-conflict refusal) is small + independent (depends on 2a).
- 2f (jagged-step support + advertising lifecycle) — depends on 2a + part
  of canonical-reorg work; can land alongside any of the others.
- 2g validates the whole.

Phase 3 (trust) is independent and can land alongside any 2.x.

Phase 4 (sparse + patch-form straddle) lands only after `#20587` is real.

## Critical files

| File | Role |
|---|---|
| `db/downloader/chaintoml_v2.go` | `ChainTomlV2` schema — `GenesisFork`, `Forks`, `ForkActivation` |
| `db/downloader/chaintoml_v2_rolling.go` | rolling publisher — populate identity fields on generate |
| `p2p/forkid/forkid.go` | `NewIDFromForks`, `NewFilterFromForks`, `GatherForks` — reused as-is |
| `node/components/manifest_exchange/provider.go` | consumer manifest-receive path — the early-reject gate |
| `db/snapcfg` / `execution/chainspec` | resolve `*chain.Config` + genesis for a chain name |

## Verification

```bash
go test -count=1 ./db/downloader/... ./node/components/manifest_exchange/...
go test -count=1 -tags p2p_integration ./node/components/integration/snapshot/...
```

End-to-end: a publisher's V2 manifest carries `genesis-fork` + the activated
`[[forks]]` list; a consumer on the same chain accepts it; a consumer on an
incompatible chain rejects it before quorum; a same-chain peer on an older
release is accepted.
