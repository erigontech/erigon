# Fork identification — implementation plan

## Context

`erigon-documents → ethereum/design/erigon-archive/fork-spec.md` is the design
(Identification, Directory layout, Cut-point placement — revised 2026-05-22).
This plan covers the **erigon-side implementation**, and only the
*identification* layer.

Explicitly **not** in this plan: the `erigon fork` CLI command, the `[view]`
section plumbing, the per-stream directory layout, and the cut/patch-file
mechanics. Those are later phases — the `[view]` read path depends on sparse
reads (`#20587`) and the fork command is a larger surface. Identification is
separable and ships first because it de-risks the manifest format and is
verifiable on chains that exist today.

## Phase 1 — manifest identity + compatibility early-reject (the initial implementation)

The grounded, no-new-dependency slice. Every chain that exists today is a single
continuous lineage, so Phase 1 ships on root chains with no view machinery: the
identity fields are populated, the consumer early-reject runs, and same-chain
peers pass it as a no-op.

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

## Later phases (sketch — not detailed here)

- **Phase 2 — `[view]` + directory layout.** `[view]` section in `ChainTomlV2`;
  partial-file fields (`partial`, `partial-parent-range`); the per-stream
  `snapshots/` subdirectory layout; consumer combine across a `[view]` parent
  link. Representation + read-path plumbing; does not need the `fork` command.
- **Phase 3 — `erigon fork` command + cut/patch mechanics.** The CLI, the
  cut-block resolver, step-boundary-snap default, full-vs-patch cut-step file
  creation. Gated on sparse reads (`#20587`) for non-local-parent consumers.
- **Phase 4 — trust across the cut.** The Authority/Content two-UCAN model
  spanning a `[view]` (fork-spec § *Trust model*).

## Sequencing

Phase 1 is independent and ships first — it puts the identity fields in the
schema before any chain needs them, and the compatibility early-reject is
verifiable against existing chains. Phases 2–4 follow; Phase 3 is gated on
`#20587`.

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
