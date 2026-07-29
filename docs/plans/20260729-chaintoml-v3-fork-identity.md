# chain.toml v3 — fork identity as one file

**Date:** 2026-07-29
**Status:** proposed
**Depends on:** chain.toml v2 spec (`20260520-chaintoml-ucan-flow-spec.md`),
fork componentization landings, debug_setFork UCAN authority
(`20260729-debug-setfork-ucan-auth.md`).

## Problem

When a fork is created — either via offline `snapshots fork-from` or
via in-process `debug_setFork` — the operator produces **three**
per-fork artefacts:

- `chain.<fork>.json` — Erigon's execution `chain.Config` (present in
  every datadir; the datadir's identity file)
- `cl-config.<fork>.yaml` — the fork's beacon-chain config (standard
  CL-spec YAML; Caplin and every other CL client read the same
  schema)
- `parent-cut.<fork>.json` — the cut record (block, hash, timestamp,
  parent chain id, etc.) — Erigon-shaped JSON

These carry parameters peers must agree on. A follower joining the
fork today has to fetch and validate all three separately. Two of
them — `cl-config` and `parent-cut` — carry information that
`chain.toml v2` already partially attests through its `[parent]`
section, but only as *identifier* fields (`cl_fork_version`,
`cl_genesis_validators_root`, `cl_config_name`) — not the actual
scalars (`SECONDS_PER_SLOT`, `DEPOSIT_CONTRACT_ADDRESS`, per-fork
epochs). And `cl-config.yaml` is not attested by any UCAN.

That leaves a gap: the CL config bytes a fork actually runs on are
not part of the chain's cryptographic identity, so two peers who
agree on the fork's chain.toml hash may still run different CL
configs.

The user's phrasing: *"if the config changes over time how do we
track it, but it does seem it's a fairly fundamental property that
needs to also be agreed."*

## What chain.toml v2 already carries

Baseline (see `db/downloader/chaintoml_v2.go`):

- Chain identity: `Version`, `GenesisFork`, `Forks[]` (activated
  continuous forks with block/time)
- File catalog: `Blocks[]`, `Domains{}`, `Caplin[]`, `Meta{}`,
  `Salt{}` — each with per-file hashes + trust markers
- Fork lineage (`Parent *ParentSection`, present only on a fork
  manifest):
  - `Chain`, `ManifestHash` (parent's v2 manifest hash), `CutBlock`,
    `CutTxNum`, `CutBlockHash`, `Name`, `NetworkID`
  - `CLGenesisValidatorsRoot`, `CLForkVersion`, `CLConfigName` —
    **identifiers only**, not the full CL config
  - `ValidParentTrustRoots[]`
- Attestation: `AuthorityUCANHash` — hash of the sidecar UCAN
  rooting the publisher's authority

So `chain.toml v2` already answers *"which fork chain? cut where?
which CL is it?"* — but not *"which CL config bytes exactly?"* or
*"what parent-cut fields other than CutBlock/CutBlockHash did we
capture?"*

## The v3 delta

Two new sections on `[parent]` (folded into `ChainTomlV2` — no new
top-level structure):

```toml
[parent]
# ... existing fields ...

[parent.cl_config]
# Bytes-inline: the standard CL beacon-chain YAML rendered as
# nested TOML. Every field a CL client reads from cl-config.yaml
# lives here. The parse projects back to YAML at Caplin load time
# (mechanical scalar copy).
config_name = "hoodi-fork-42"
preset_base = "mainnet"
seconds_per_slot = 12
deposit_chain_id = 560048
deposit_contract_address = "0x00000000219ab540356cbb839cbe05303d7705fa"
# ... every standard CL scalar ...

# sha256 over the canonical YAML rendering of the bytes above.
# UCAN attestation binds to this hash so a peer can verify the CL
# config bytes carry a signature from the fork's trust root.
sha256 = "cafebabe...deadbeef"

[parent.parent_cut]
# Erigon-shaped ParentCut record — cut-block hash, timestamp,
# parent chain id, parent manifest name. Small (<1KB), inlined as
# nested TOML to keep chain.toml self-contained.
parent_chain = "hoodi"
parent_chain_id = 560048
cut_block = 3164608
cut_block_hash = "0x1234..."
cut_block_timestamp = 1735689600
cut_block_parent_hash = "0xfedcba..."
parent_manifest_name = "chain.v2.<enr-fp>.<seq>.toml"
parent_manifest_hash = "20004fef6f6b652bde5f7c20e67e33cbc3e059d3"

sha256 = "a1b2c3...789"
```

**Design rules for the two sections:**

- **Every field inlined as TOML scalars** — no base64-encoded blobs,
  no external references. Chain.toml is one file; a peer reads it
  and knows the fork's identity end-to-end.
- **Per-section `sha256` field** — computed over a canonical
  serialisation of the section's scalars. UCAN attestations bind to
  these hashes; a peer can verify a CL config section carries the
  fork trust root's signature without re-hashing the whole
  chain.toml.
- **Redundancy with existing `[parent]` fields is expected and OK.**
  `CutBlock` appears both at `[parent].cut_block` (existing) and
  `[parent.parent_cut].cut_block` (new); mismatch is a parse-time
  error. The existing fields stay for back-compat with v2 readers;
  the new sections layer on top.

## Two more things that should carry a section

Beyond the fork case, the same shape generalises to any **negotiated
variable** — a parameter peers on the same chain must agree on but
that isn't in the standard execution `chain.Config`.

Candidates (deferred until we have concrete demand — sketched here
so the schema doesn't lock us out):

- `[chain.execution]` — extended EL flags not in `chain.Config`
  (parallel-exec knobs, VM feature flags). Today: implicit,
  operator-configured, silently divergent. With v3: attested.
- `[chain.retire]` — retire cadence + delayed-merge windows. Today:
  implicit, per-operator. With v3: coordinated.

The v3 spec **defines these sections as reserved**; they're empty
placeholders until we have a bug that needs them.

## Emitting projections at load time

Peers exchange one file. Local processes still consume the shapes
they read today:

- `applyForkWriteCLConfig` in `backend.go` (and the sibling in
  `forkfrom`) reads `[parent.cl_config]` from chain.toml and writes
  `cl-config.<fork>.yaml` — a projection, not the source of truth.
- `applyForkWriteParentCut` reads `[parent.parent_cut]` and writes
  `parent-cut.<fork>.json` — same pattern.
- `applyForkChainConfigPersist` continues to write
  `chain.<fork>.json` unchanged (Erigon's chain.Config is a v3
  reader/writer concern separately).

The projections are **deterministic** (mechanical scalar copy, no
information added or dropped). A byte-identity test — mirror of the
existing `TestWriteForkCLConfig_TwoEntryPointsMatch` — pins that
both paths (offline fork-from + in-process debug_setFork) produce
identical projections from identical chain.toml v3 input.

## Migration from v2 → v3

Not a hard-fork. `ChainTomlV2` has a `Version int` field that today
is `2`; v3 sets it to `3`. Parsers upgrade in place:

- Read v2: no `[parent.cl_config]` / `[parent.parent_cut]` sections
  → fall back to the existing separate-file loads (`cl-config.yaml`
  + `parent-cut.json` on disk).
- Read v3: prefer the inline sections; the separate files are
  optional back-compat outputs.
- Emit v3 by default from the next fork componentization landing;
  emit v2 only when `--chaintoml.compat=v2` is set (drop after one
  release cycle).

The `parent_manifest_hash` chain (this fork's parent, that fork's
parent, back to a root chain's UCAN-attested v3) forms the
**hash-chain of chain identity over time** — the answer to *"how do
peers agree when config changes."* Each version pins the previous.
A peer walks the chain from a known-good hash back to the current
one, verifying UCAN attestations at each hop.

## Peer negotiation model

Today: two peers exchange chain.toml hashes; if equal, they agree
on the file catalog + fork lineage. If unequal, one is on a
different fork or a different manifest generation.

With v3: same exchange, same hash. But now agreement covers the CL
config + parent-cut bytes too. A peer whose `cl_config.sha256`
doesn't match ours is running a different CL — surfaced at
manifest_exchange time, not much later when Caplin misbehaves.

No new negotiation protocol. Just wider content under the same
hash.

## What this is not

- **Not a chain-config editor.** v3 attests what a fork is. Changing
  a fork's CL config once distributed requires a new v3 manifest
  (new hash, new UCAN attestation, chained to the old one). That's
  the point — silent config drift becomes impossible.
- **Not a discovery mechanism.** Peers still discover each other via
  ENR / bootnodes / manifest_exchange. v3 doesn't touch that layer.
- **Not a genesis file.** CL genesis (`genesis.ssz`) stays a
  separate artefact — it's large (~10MB+), content-addressable via
  its own hash, and referenced *by* the CL config's
  `GENESIS_ROOT` (already standard).

## Rollout

- **Phase 1 (this doc):** design agreed; no code.
- **Phase 2:** `ChainTomlV2.Version` bump to 3, new fields on
  `ParentSection`, v2 back-compat parse path, v3 emit path in
  `forkfrom` + `applyPostSwapHooks`. TDD via extended
  `TestWriteForkCLConfig_TwoEntryPointsMatch` and a new
  `TestParseChainTomlV3_BackCompat` covering v2 → v3 read.
- **Phase 3:** UCAN attestation extended to cover the per-section
  hashes — the fork-authority UCAN carries
  `chain.v3:parent-cl-config-hash:<hex>` and
  `chain.v3:parent-cut-hash:<hex>` alongside the existing content
  cap. A follower whose chain.toml v3's section hashes don't match
  what the UCAN attests rejects the manifest.
- **Phase 4:** loading path — Caplin + Provider read the inlined
  sections in preference to the separate files; separate files
  become v2-compat projections only.
- **Phase 5:** deprecate `--chaintoml.compat=v2` emit; drop the
  separate-file back-compat one release later.

## Non-goals for Phase 1

- Extending v3 to non-fork chains (root chains today have no
  `[parent]` section — the fold makes no sense there). If we ever
  want root-chain CL config attestation, that's a distinct schema
  change.
- Encoding negotiation of dynamic parameters (gas limit, priority
  fee model). Those change per-block, not per-chain-identity, and
  live in consensus itself.
- Multi-CL-client YAML fixup (e.g. Lighthouse-specific fields
  Erigon's beacon config parser ignores). The CL config we emit
  is spec-format; anything a client needs beyond spec is that
  client's config, not the fork's.
