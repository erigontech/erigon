# Chain manifest SSZ schema — derivation + cutover plan

## Design discipline (user, 2026-05-24)

> "The speced ssz container is the chain format designed rather than
> evolved through the process. We should assume it will eventually be
> agreed through consensus."

This is the load-bearing framing for the rest of the doc.

The SSZ container **IS the chain manifest format** — not a
serialization of what `ChainTomlV2` happens to be today. We are
deriving the format from first principles, with the working
assumption that it will eventually go through an EIP-style consensus
review and be adopted across clients. Every field present has to
justify itself on that basis.

Consequences for the design:

1. **Drop TOML-organic artefacts.** Fields and shapes inherited from
   the V1→V2 evolution that don't carry first-principles weight should
   be reconsidered, not preserved. Examples to re-evaluate below.
2. **Schema is the authority; Go struct is a representation.** Today
   the Go struct is the canonical declaration and TOML/SSZ/etc. are
   serializations. After this work, the SSZ spec is the canonical
   declaration; the Go struct is a Go-language representation that
   conforms.
3. **Design for an external review audience.** Field names, max
   bounds, enums, optionality decisions should be defensible as a
   consensus-agreed artefact, not a Go convention. Strict naming;
   tight bounds; deliberate enumerations.
4. **Separate "what's in the manifest" from "how each operator
   trusts it".** The manifest is the artefact every party agrees
   on; per-operator trust assessments (UCAN chains, local trust
   roots, per-file confidence levels) live OUTSIDE the manifest as
   side-channels keyed to manifest identity.
5. **Anticipate forward-compatibility.** Use `ProgressiveContainer`
   so the V2 schema's Merkle gindices stay stable when a future V3
   adds fields. Resist the urge to add speculative fields "for the
   future" — V3 can add them losslessly.

The schema below is a first draft applying this discipline. Open
items at the end flag fields that may need further first-principles
review.

## Why SSZ on the wire

Today the snapshot manifest is TOML on the wire: `MarshalV2(*ChainTomlV2)`
produces TOML bytes; those exact bytes go into the BitTorrent torrent.
The format conflates three layers — schema (Go struct), wire (torrent
payload), print (human-readable). The migration separates them: wire
becomes SSZ; print stays TOML for human inspection.

The single load-bearing property motivating SSZ on the wire is
**canonical marshalling without formatting conventions**. SSZ produces
one byte sequence for a given value — no indentation, no key-order
ambiguity, no escape-form variability. Two independent implementations
marshal identical bytes → identical hash-tree-roots → consensus on the
manifest's identity is possible at all. TOML and JSON cannot offer this
without a separate canonicalization layer (which itself becomes a
consensus-relevant artefact); SSZ bakes canonicality into the encoding.

Once SSZ is the wire format, three derived properties follow:
hash-tree-root computability, future consensus-state inclusion
(`snapshot_manifest_root` alongside `historical_summaries`), and
Merkle-proof support (a light client can verify one file's hash
without the whole manifest — directly relevant to Phase 4 sparse
fetch).

See [[chain-ssz-consensus-component-endpoint-2026-05-23]] for the
broader endpoint reasoning.

## Pattern conventions (from EIP / consensus-specs research)

Canonical SSZ specification format mirrors the consensus-specs repo:
Python `dataclass`-style declarations inside fenced code blocks.

- **Containers**: `class Name(Container):` with typed fields.
- **Fixed-size bytes**: `Bytes20`, `Bytes32` aliases (NOT
  `List[byte, 20]` — fixed-size is cheaper to hash + matches the
  `BLSPubkey: Bytes48` family).
- **Variable-length bytes / strings**: `ByteList[N]` (alias for
  `List[byte, N]`); the bound is mandatory.
- **Optional fields**: `Union[None, T]` — selector 0 = absent,
  selector 1 = present. (Consensus-specs default for nullable.)
- **Maps**: SSZ has no native map; use `List[KVContainer, N]` sorted
  by key on encode. Precedent: EIP-6404's `access_list:
  List[AccessTuple, MAX_ACCESS_LIST_SIZE]`.
- **Naming**: type names PascalCase; field names snake_case;
  constants SCREAMING_SNAKE_CASE.
- **Constants**: `uint64(2**N)` form at the top of the schema doc.

For schema-evolution safety we wrap the top-level container as
`ProgressiveContainer(active_fields=[1,...,1])` per EIP-7688 —
appending new fields in V3 keeps existing V2 fields' Merkle gindices
stable, so proofs against V2 stay valid across future schema additions.

## Constants

```python
# Bound the schema for canonical hash-tree-root. Tight bounds; producer
# side is controlled, so generous headroom isn't necessary. Each bound
# should carry a documented rationale before locking (open item 5).

MAX_TOTAL_FILES      = uint64(2**20)   # 1M files across all kinds + domains; ~256K per chain today
MAX_FORK_ACTIVATIONS = uint64(2**6)    # 64 forks in a chain's lifetime
MAX_NAME_LEN         = uint64(2**8)    # 256-byte file name
MAX_KEY_LEN          = uint64(2**8)    # 256-byte KVEntry key
MAX_VALUE_LEN        = uint64(2**12)   # 4 KiB KVEntry value
MAX_CHAIN_NAME_LEN   = uint64(2**6)    # "mainnet", "mainnet-fork-23760000"
```

## Custom types

| Name | SSZ equivalent | Description |
|---|---|---|
| `Version`     | `Bytes4`  | Schema version stamp (`0x00000002` for V2). |
| `ChainId`     | `uint64`  | EL chain id. |
| `NetworkId`   | `uint64`  | p2p network id (distinct from `ChainId` for forks). |
| `BlockNumber` | `uint64`  | EL block height. |
| `TxNum`       | `uint64`  | Cumulative txnum across the chain. |
| `InfoHashV1`  | `Bytes20` | BitTorrent v1 info-hash. |
| `Hash32`      | `Bytes32` | Generic 32-byte hash (block hash, validators root, manifest root). |
| `ForkVersion` | `Bytes4`  | CL fork version (e.g. `0x10000001`). |
| `GenesisFork` | `Bytes4`  | CRC32(genesis_hash). |
| `ValuesContent` | `uint16` | Discriminator for `ValuesFile.content`. Registry-keyed; covers both state-domain values (Accounts=0x0001, Storage=0x0002, Code=0x0003, Commitment=0x0004, Receipt=0x0005, RCache=0x0006, LogAddrs=0x0007, LogTopics=0x0008, TracesFrom=0x0009, TracesTo=0x000A, ...) and sequence-file layers (ExecutionBlock=0x0100, ConsensusBeacon=0x0101, ...). Zero = unknown. |
| `StateContent`  | `uint16` | Discriminator for `HistoryFile.content` and `IndexFile.content` — state domains only (subset of `ValuesContent` 0x0001-0x00FF). |
| `IndexTarget`   | `uint8`  | Discriminator for `IndexFile.target`: Values=0 (indexes target the `.kv` data), History=1 (indexes target the `.v` log), InvertedIndex=2 (indexes target the `.ef` log). |
| `SaltSubtype`   | `uint8`  | Discriminator for `SaltFile.subtype`: BlocksSalt=0, StateSalt=1, ... (registry-keyed). |
| `ChainName`     | `uint16` | enum keyed off the known-chains registry (Mainnet=1, Sepolia=2, Holesky=3, ...). New chains added as new registry entries. Zero = unknown / not registered. |
| `HardForkName`  | `uint16` | enum keyed off the EL hardfork registry (Berlin, London, Shanghai, Cancun, Prague, ...). Zero = unknown. |

## The combining principle — two axes of change (user, 2026-05-24)

> **Read first**: the consensus-related framing below describes a
> consistent operational framework, NOT a claim about current
> consensus-state inclusion. The framework is what the system uses
> today via the negotiated-variable model (Layer-1 quorum + per-chain
> trust roots + UCAN auth, see
> [[fork-trust-root-model-2026-05-24]]). Consensus enshrinement is
> one possible future implementation of the *same* framework; it may
> never happen. The negotiated mechanisms must not be removed. See
> [[chain-two-axes-of-change-2026-05-24]] §"Critical framing" for
> the full grounding.

The deepest architectural axiom beneath this schema design:

> "There are 2 theoretical axes of change interacting in a chain.
> Time (the sequence of events), and value versions — the sequence
> of data. Consensus fixes both time & value. … On this axis
> consensus components can only have one value, they are fixed in
> time, state components can still have many values, this is what
> the canonical archive records."
>
> "It's the model that is used for combining state & blocks in the
> value/history axes."

**Two axes:**

1. **Time axis** — the sequence of events (blocks for EL, slots for CL)
2. **Value axis** — the sequence of data along the time axis

**Consensus fixes both.** When a position is finalized, both axes are
nailed: one canonical event and one canonical resulting state. The
canonical archive records the time-history along a single fixed axis.

**Cardinality along the fixed time axis differs by component class:**

| Component class | Value-axis cardinality at each time point | Schema families used |
|---|---|---|
| Consensus components (EL blocks, CL beacon blocks) | 1 — consensus fixed the block | Values only |
| State components (accounts, storage, code, commitment, ...) | N — many values per key across the axis | Values + History |

**Combining principle**: state and blocks share one schema. They're
not separate artefact families — they're projections of the same
(time, value) chain co-evolution, distinguished by their cardinality
along the value axis. The Values and History families apply
uniformly; for cardinality-1 components History collapses onto Values
(it would tautologically equal Values) and is omitted. For
cardinality-N components both are needed: Values for the
snapshot-at-coordinate, History for the per-key sequence.

This is **why** the four-family structure (Values/History/Indexes/Salt)
is structurally natural rather than a categorisation convenience: it
falls out of cardinality + derivation roles along the consensus-fixed
axes, not from a Erigon-internal file-shape taxonomy.

### The state commitment IS the bi-axis fixing (user, 2026-05-24)

The state commitment (the block header's `stateRoot`, recorded in
the schema as a `ValuesFile`'s `proof_root` + `at_block` + `at_tx_num`
anchors) is the cryptographic witness of the bi-axis fixing. A
single signed statement says:

> "At time-axis coordinate N (this block), the value axis is at
> state X (this trie root)."

The header anchors the time axis (consensus-fixed event ordering);
the state root anchors the value axis (consensus-fixed data
evolution). Joining them in one signed structure binds the two axes
cryptographically.

This explains the verification semantics:

- A validator checking `proof_root ≡ header[at_block].stateRoot` is
  verifying the bi-axis fixing — confirming the manifest's claim
  about the value axis matches the consensus-fixed claim from the
  time axis.
- A partial-block commitment (recording `at_tx_num` < block's
  `MaxTxNum`) claims a value-axis position at a *mid-block* time
  coordinate where consensus has not fixed the time axis; there is
  no header to bi-axis-bind against. Such commitments are
  unverifiable against the chain even though they're well-formed
  cryptographically. The block/slot-aligned model
  ([[block-slot-aligned-storage-model-2026-05-24]]) closes this gap
  by construction: every `at_tx_num` coincides with a block
  boundary, so every commitment bi-axis-binds against a real
  consensus-anchored time coordinate.
- A future chain-level consensus endpoint that includes
  `hash_tree_root(ChainManifestV2)` alongside `historical_summaries`
  promotes the manifest itself to a bi-axis statement — the
  archive's value-axis claims are committed under the consensus-
  fixed time-axis cursor (cf.
  [[chain-ssz-consensus-component-endpoint-2026-05-23]]).

See [[chain-two-axes-of-change-2026-05-24]] for the full memo
including the commitment-as-bi-axis-fixing coda.

## Canonicality (user, 2026-05-24)

## Canonicality (user, 2026-05-24)

> "The model is quite natural as they are never historical in the
> archive which is canonical only. For in-flight consensus they
> actually do have a history, it's the previous versions of that
> block entity discriminated by their hash."

The schema describes **archived data, which is canonical-only by
construction**. For each `(family, content, range)` coordinate that
appears in a manifest, the file is canonical for that coordinate.
This load-bearing property explains the family structure:

| Family | Canonical role |
|---|---|
| **Values** | The canonical truth at a coordinate — state at a step, or blocks in a range. Block files are value-only because every archived height has exactly one canonical block. |
| **History** | The canonical log of how the truth evolved. State-only, because state is where canonical evolution is recorded in the archive. |
| **Indexes** | The canonical accessor over a canonical source — derivable from Values/History + Salt; same source bytes + same Salt deterministically yield the same Index file. |
| **Salt** | The canonical seed enabling deterministic Index construction across all publishers. |

**Live consensus is where history-of-entities lives, and it sits
outside this schema.** Competing block versions at the tip, orphans,
reorged forks — all discriminated by hash, all in-flight. The
snapshot manifest never describes them; they exist only on the live
consensus channel (CL gossip, fork-choice). The archive and the
live channel are different artefact families; the schema covers only
the archive.

**Canonical does NOT mean complete.** A manifest describes the
files a particular publisher holds and is willing to distribute —
not "the full canonical archive of this chain" (user, 2026-05-24:
"the history is trimmed, as for example is the commitment history
if we distribute the archive without it"). A publisher running in
`--prune.mode=minimal` legitimately omits commitment history;
their manifest still describes a canonical archive — just one with
fewer files. Consumers needing the trimmed entries discover them
from publishers that do retain them. Trimming is a publisher-side
policy choice; it does not invalidate per-coordinate canonicality.

Two consequences:

1. **Per-publisher canonicality** — every entry in this publisher's
   manifest is canonical for its coordinate; absent entries are
   "not held," not "not canonical."
2. **Chain-wide canonicality (across publishers)** — established by
   the Layer-1 quorum view: a `(family, content, range)` coordinate
   is canonical at the chain level when ≥ Q distinct UCAN-verified
   publishers report the same info-hash for it. Trimming reduces
   coverage at one publisher but doesn't break Layer-1 quorum —
   other publishers that retain the entry still vote on its
   canonicity.

This framing means the four-family distinction is structurally
natural rather than a categorisation convenience: each family
describes a *role in the canonical archive*, not an Erigon-internal
file shape. And each role is independently trimmable per publisher.

## First-class file families (user-agreed 2026-05-24)

| Family | What it holds | Today's extensions | Notes |
|---|---|---|---|
| **Values** | Canonical data at a range | state `.kv`; EL block `.seg`; CL beacon `.seg` | Block files are value-only types (no separate history, indexes, or salt for blocks). |
| **History** | Append-only log of value evolution + its inverted-index | state `.v` + `.ef` (these go together: `.v` is the value log, `.ef` is the txnum→value inverted index over the log) | State-only. |
| **Indexes** | Derived lookup structures (rebuildable from Values/History; loss is recoverable) | `.bt`, `.kvi`, `.vi`, `.efi` | State-only. `target` field discriminates which underlying file the index targets. |
| **Salt** | Deterministic chain-wide seed material — an input to Indexes construction | `salt-blocks.txt`, `salt-state.txt` | No range. Different from Indexes because it's a source input, not a derivation. |

## Containers

```python
class ForkActivation(Container):
    # Exactly one of block / time is non-zero.
    name:  HardForkName             # enum (Berlin=1, London=2, Shanghai=3, ...); zero = unknown
    block: BlockNumber              # 0 when time-based
    time:  uint64                   # 0 when height-based


# File identity is STRUCTURAL — per-family containers describe what
# fields actually apply. The on-disk filename is reconstructible by
# a producer/consumer-shared filename construction function (see
# § "Filename construction" below); the schema describes structural
# identity, not strings.
#
# Block files are value-only (no separate history, indexes, or salt
# for blocks); confirmed 2026-05-24. They appear as ValuesFile entries
# with content = ExecutionBlock / ConsensusBeacon.

class ValuesFile(Container):
    # The canonical data at a range. Covers both state-domain primaries
    # (.kv) and sequence-file layers (EL .seg, CL beacon .seg).
    content:     ValuesContent           # state domain OR sequence layer
    range_from:  uint64                  # txnum for state; block for EL; slot for CL
    range_to:    uint64
    info_hash:   InfoHashV1
    # Proof anchors — populated when the file records a state-root
    # anchor (today: commitment-domain primaries). All zero when none.
    proof_root:  Hash32
    at_block:    BlockNumber
    at_tx_num:   TxNum


class HistoryFile(Container):
    # Append-only log of value evolution paired with its inverted-index.
    # State-only. The .v log and the .ef inverted index logically pair;
    # in the on-wire form each emits a separate HistoryFile entry whose
    # filename construction reveals which physical extension it is.
    content:     StateContent            # state domain (subset of ValuesContent)
    range_from:  uint64                  # txnum
    range_to:    uint64
    info_hash:   InfoHashV1
    # No state-root anchor — history doesn't anchor state at any single
    # block; it logs values across the range.


class IndexFile(Container):
    # Derived lookup structure over a Values, History, or InvertedIndex
    # target file. Rebuildable from its target + Salt; loss is recoverable.
    # State-only.
    content:     StateContent            # state domain
    target:      IndexTarget             # Values | History | InvertedIndex
    range_from:  uint64                  # matches the target file's range
    range_to:    uint64
    info_hash:   InfoHashV1


class SaltFile(Container):
    # Chain-wide deterministic seed material — an input to Indexes
    # construction. Not range-keyed; not derived. First-class because
    # losing it breaks reproducibility of Indexes across publishers.
    subtype:     SaltSubtype             # BlocksSalt | StateSalt
    info_hash:   InfoHashV1


class FileEntry(Container):
    # Discriminated union over the four first-class file families.
    # Each family carries only the fields that apply to it.
    file: Union[ValuesFile, HistoryFile, IndexFile, SaltFile]


class KVEntry(Container):
    # Used for any key-value carry beyond what FileEntry expresses
    # (today: meta keys mapping to file names). Sort by key on encode
    # for canonical root.
    key:   ByteList[MAX_KEY_LEN]
    value: ByteList[MAX_VALUE_LEN]


class ParentSection(Container):
    # Present only on a fork manifest. EL + CL identity together let a
    # fork-follower wire both halves at bootstrap.
    chain:                       ChainName        # parent chain (enum; e.g. Mainnet=1, Sepolia=2, ...)
    manifest_hash:               InfoHashV1       # parent's manifest at cut time
    cut_block:                   BlockNumber
    cut_tx_num:                  TxNum
    cut_block_hash:              Hash32
    cl_genesis_validators_root:  Hash32
    cl_fork_version:             ForkVersion


class ChainIdentity(Container):
    # A stand-alone SSZ manifest must self-identify which chain it
    # describes — required when the manifest becomes a consensus
    # component (CL state inclusion). chain_id stays = parent for a
    # fork's EL (replay protection); network_id distinguishes the
    # fork's p2p network.
    chain_name:   ChainName    # enum keyed off known-chains registry; canonical identifier alongside chain_id
    chain_id:     ChainId      # EL chain id (parent's for forks)
    network_id:   NetworkId    # p2p network id
    genesis_hash: Hash32       # the content-addressed anchor
    genesis_fork: GenesisFork  # CRC32(genesis_hash); derivable but cheaper to include


# Top-level manifest. Designed from first principles, not evolved from
# the V1/V2 TOML organic shape. ProgressiveContainer per EIP-7688 keeps
# V2 Merkle gindices stable when V3 appends fields.
class ChainManifestV2(ProgressiveContainer(active_fields=[1] * 5)):
    version:  Version                                  # always 0x00000002 for V2
    identity: ChainIdentity                            # self-identifies the chain
    forks:    List[ForkActivation, MAX_FORK_ACTIVATIONS]
    parent:   Union[None, ParentSection]               # absent on root chains
    files:    List[FileEntry, MAX_TOTAL_FILES]         # every file, sorted by (kind, domain, range_from)
```

Note the collapse: the top-level container is FIVE fields. Per-section
top-level groupings in the V2 TOML form (`blocks`, `caplin`, `meta`,
`salt`, `domains`) were Erigon-internal categorisation —
client-implementation naming that doesn't survive a consensus-bound
lens. The schema replaces them with ONE `files` list whose entries
are a discriminated union over four first-class families: Values,
History, Indexes, Salt. The (family, content, range) tuple is the
complete structural identity; the on-disk filename is reconstructible
from these fields.

Producers MUST sort `files` deterministically before encoding so
`hash_tree_root` is canonical across publishers. Sort key:
(family-tag, content, target [Indexes only], range_from). Salt
entries sort by (family-tag, subtype) since they have no range.

## Filename construction

The on-disk filename is a producer/consumer-shared deterministic
function of the FileEntry's structural fields plus the publisher's
naming-convention selection (rounded vs literal — see § "Block/slot-
aligned model" below for the convention split). Conceptually:

```
ConstructFilename(entry, version, convention) =
  case entry.file of:
    ValuesFile(content, range_from, range_to, ...):
       if content is state domain:
         "v<version>-<content_name>.<from>-<to>.kv"
       if content == ExecutionBlock:
         "v<version>-<from>-<to>-<subkind>.seg"
       if content == ConsensusBeacon:
         "v<version>-<from>-<to>-beacon.seg"
    HistoryFile(content, range_from, range_to):
       "v<version>-<content_name>.<from>-<to>.v"  # the .v log
       "v<version>-<content_name>.<from>-<to>.ef" # the paired inverted index
       (the manifest emits two HistoryFile entries — one .v, one .ef —
       sharing content + range but with distinct info_hashes; the family
       tag alone doesn't disambiguate so an additional sub-discriminator
       in HistoryFile is an open item — see §"Open items")
    IndexFile(content, target, range_from, range_to):
       case target:
         Values:         "v<version>-<content_name>.<from>-<to>.bt" / ".kvi"
         History:        "v<version>-<content_name>.<from>-<to>.vi"
         InvertedIndex:  "v<version>-<content_name>.<from>-<to>.efi"
       (BTree vs KVI both target Values — open item; may need a sub-
       discriminator on IndexFile too)
    SaltFile(subtype):
       case subtype:
         BlocksSalt: "salt-blocks.txt"
         StateSalt:  "salt-state.txt"
```

The function is producer/consumer-symmetric. New file shapes added via
registry update + matching ConstructFilename arm in both halves.

### Notes on the schema

- **Map → List of KV containers** (consensus-specs convention; EIP-6404
  precedent): Erigon-internal `map[string]string` shapes all become
  typed lists. Producers MUST sort by key on encode (SSZ does not
  sort) so the hash-tree-root is deterministic across producers.
- **Hex strings → fixed-size bytes**. `manifest_hash`, `cut_block_hash`,
  `cl_genesis_validators_root`, `cl_fork_version`, `info_hash`,
  `proof_root` all drop their hex encoding and become
  `Bytes20`/`Bytes32`/`Bytes4`. Hex stays the *print* form;
  on the wire it's bytes.
- **Optional Parent** = `Union[None, ParentSection]`. Selector byte
  0 = absent (root chain); 1 = present (fork). Cleaner than the
  always-present-zero alternative; absence is explicit.
- **Files are a `Union` of per-family containers**, not a single flat
  shape. Each first-class family (Values, History, Indexes, Salt) gets
  the fields that apply to it; Salt carries no range, History carries
  no anchors, Indexes carries a target discriminator. Sequence files
  (EL/CL blocks) appear under ValuesFile because they are value-only
  types — no history, no indexes, no salt apply to blocks.
- **Filename strings are absent from the schema.** File identity is
  structural — `(family, content, target, range)`. The on-disk
  filename is a deterministic function of structural fields plus the
  naming convention; consumers reconstruct it. Strings only exist as
  print-form, never on the wire.
- **All free-string identifier fields are enumerated against a
  registry.** `ChainName`, `HardForkName`, `ValuesContent`,
  `StateContent`, and the various sub-discriminators are uint16/uint8
  enums keyed off external registry artefacts maintained alongside
  the schema. Adding a new chain / new domain / new hardfork name is
  a registry update, not a schema change.
- **No `version` per sub-container**. The top-level `version: Bytes4`
  plus `ProgressiveContainer` shape carries schema-version semantics
  for the whole tree.

### Deliberately excluded (per the design-discipline framing)

These were present in `ChainTomlV2` but should NOT be in the
consensus-bound schema. Each is documented as a side-channel
maintained externally to the manifest.

- **`authority_ucan_hash`** — UCAN is **transport infrastructure**
  (user, 2026-05-24), not part of the manifest's schema. The
  architectural stratification:

  | Layer | Concern | What lives here |
  |---|---|---|
  | Schema | What the manifest means | `ChainManifestV2` (this doc) |
  | Wire | How it's encoded | SSZ bytes (canonical, hash-tree-rootable) |
  | Transport | How the bytes move + who's authorised to serve them | BitTorrent + UCAN today; could be HTTP + signed-by-PKI; could be on-chain inclusion + validator attestation |
  | Trust | Who vouches for the identity | Cryptographic attestations binding `hash_tree_root(manifest)` |

  UCAN sits at transport: it authenticates an operator's right to act
  as a publishing peer in the BitTorrent mesh AND vouches for the
  bytes they serve under that authority. The manifest itself doesn't
  know UCAN exists; UCAN-attested BitTorrent is one of several
  possible transports.

  Direction of reference matters: the manifest does NOT reference its
  attestations; **the attestation references the manifest's
  hash-tree-root**. A Content UCAN's capability binds
  `hash_tree_root(ChainManifestV2)`; multiple attestations can wrap
  the same root from different operators, each independent of the
  others and of the manifest itself. Same pattern as CL attestations
  → beacon block roots.
- **`Trust string` on every file entry** — per-operator confidence
  level ("verified" / "consensus" / "unverified"). Doesn't belong in a
  consensus-agreed artefact; each consumer derives trust from
  attestations against the manifest root, not from a field the
  publisher stamps. Removed from `FileEntry`.
- **`is_partial_block` flag** — was a convenience field encoding
  partial-block-ness for validators inferring it from filename + step
  alignment. With manifest-driven validators (see
  [[manifest-driven-not-convention-driven-2026-05-23]]) the property
  is derivable from `at_tx_num` vs the block's `MaxTxNum`. Not stored.
  If derivation cost ever matters, a `derive_partial_block()` helper
  computes it; the wire form stays minimal.
- **Client-implementation kind names** — Erigon's `KindBlock` /
  `KindCaplin` / `KindMeta` / `KindSalt` are renamed under the
  consensus-bound lens: `Block` → `Execution` (an execution-layer
  file: header, body, transactions, receipts), `Caplin` → `Consensus`
  (a consensus-layer beacon-archive file; "Caplin" is Erigon's CL
  implementation name and has no place in a schema other clients
  would adopt). Today's separate top-level sections (`blocks`,
  `caplin`, `meta`, `salt`) similarly were Erigon-internal
  categorisation; the manifest becomes a single typed list of files
  with a `kind` enum that names the layer + role, not the producer.

### Open items needing first-principles review

1. **Should `forks` carry name strings, or just sorted activation
   coords?** Names ("ShanghaiTime", "CancunTime") are informational
   today. A consensus-agreed schema doesn't need them — the fork ID
   derivation is positional. Names make logs readable; they cost
   ~16 bytes each. Lean toward keeping but worth confirming.
2. **Should `domains` carry the domain name** (`"accounts"`,
   `"storage"`, ...) as a string, or as an enum? Today's TOML uses
   strings; the set is small and closed. Enum (uint8) is cheaper +
   more disciplined. Recommendation: enum.
3. **Should chain identity include `genesis_hash`** in addition to
   `chain_name` + `chain_id`? `genesis_hash` is the content-addressed
   anchor; `chain_name` is a label. A consensus-agreed schema
   probably wants the hash too — it's what `GenesisFork` is derived
   from. Recommendation: add `Hash32 genesis_hash` to `ChainIdentity`.
4. **Should `ParentSection` carry the CL config name** (`msf-0`)?
   It's a label; the load-bearing fields are the CL identity
   (validators root, fork version). Could drop the name; defer to
   confirm value of human-readable CL identifier.
5. **Maximum bounds** — the constants in §Constants are first-pass
   estimates. A consensus-agreed schema needs each bound to have a
   "based on N chains of M years of history" rationale documented.
   Add reasoning column to the constants table before locking.

## Cutover plan

### Cutover boundary — LOCKED 2026-05-24

**SSZ migration is the FINAL change before PR-to-main, followed by a
full retest cycle, then the PR opens.** The order:

1. Phase 2 (Fork + Unwind) completes.
2. Other post-functional work proceeds (security audit, performance,
   scaling).
3. SSZ migration lands as the last pre-PR change. Dual-publish + ENR
   carries both info-hashes; consumer prefers SSZ + TOML fallback.
4. Full retest cycle on the SSZ-shipping binary:
   - Sepolia soak (the same cascading-clients pattern we've been
     running)
   - Harness scenarios (snapshot-flow integration suite)
   - Assertoor playbooks where applicable
   - All -ve and +ve test suites green
5. PR opens with validated SSZ-on-wire shipping code.

Why this sequencing (not "PR ships SSZ"): SSZ is a substantive change
to the wire format. It deserves its own soak validation as a discrete
risk, not bundled into the PR submission. The PR then carries SSZ as
already-validated code rather than as a new change reviewers must
trust. Reduces both real risk and review-time risk.

Alternative boundaries considered and rejected:

| Boundary | Why rejected |
|---|---|
| End of Phase 2 (alongside Fork + Unwind) | Pushes format work into Phase 2's critical path; risks Phase 2 timing for a benefit that's separable |
| History-network integration (post-PR) | Loses the period during PR-to-main when SSZ would be foundational for incoming consumers; pushes the bigger change later for no architectural payoff |

### Cutover mechanism (dual-publish during transition)

EIP-6404's "single format per channel" guidance argues against
multiplexing on the same channel. We avoid that by treating the SSZ
and TOML manifests as **separate artefacts** with separate torrents:

- Publishers emit BOTH during the transition window:
  - `chain.v2.<enr-fp>.<seq>.toml` (current — print + TOML wire)
  - `chain.v2.<enr-fp>.<seq>.ssz` (new — SSZ wire; deterministically
    derived from the same Go struct)
- The ENR's chain-toml entry carries BOTH info-hashes (extend
  `enr.ChainToml` to add `SSZInfoHash`).
- Consumers prefer SSZ when available; fall back to TOML when the
  peer hasn't advertised an SSZ hash yet.
- After enough consumers support SSZ (operational measurement +
  release threshold), publishers drop TOML emission.

The Content UCAN re-targets to bind `hash_tree_root(SSZ)` instead of
`sha256(toml_bytes)` for the SSZ artefact; for TOML it continues
binding `sha256(toml_bytes)`. Until cutover, both signatures coexist
in their respective sidecars.

This pattern is similar to libp2p / devp2p multistream-select
versioning rather than a hard fork cut — appropriate because the
manifest isn't itself a consensus artefact yet (it becomes one only
at the chain.ssz endpoint).

### What lands in each phase

| Phase | SSZ-related work |
|---|---|
| **Phase 2 (in progress)** | Schema design (this doc). Schema fields added to Go structs keep nil-able / sortable shapes so SSZ swap is cheap. No on-wire change. |
| **Phase 2g (tests)** | Round-trip tests: Go struct → SSZ → Go struct identity; SSZ encoding is deterministic across runs; hash-tree-root is stable. |
| **Pre-PR-to-main hardening** | Implement `MarshalSSZ` + `ParseSSZ` for `ChainManifestV2`. Add dual-publish path in the publisher; add SSZ-prefer / TOML-fallback in the consumer. |
| **PR-to-main itself** | Lands with both formats live; default consumer prefers SSZ. |
| **Post-PR soak** | Operational measurement of SSZ adoption among consumers. |
| **Post-functional cleanup** | Drop TOML emission once the soak shows >X% consumers on SSZ (threshold TBD). |
| **History-network integration** | `hash_tree_root(SSZ)` becomes the canonical content identifier; published Content UCANs bind it; CL state inclusion explored. |

## Pickup

- This doc is the SSZ format derivation; treat as the **draft schema
  spec** going forward.
- Validate the schema with the user before implementation: open
  questions in the §Open schema questions section need answers.
- Implementation is sequenced to land alongside PR-to-main, not
  during Phase 2 proper.
- Schema additions made during Phase 2 (e.g. Phase 2a's
  `ParentSection`) should be SSZ-friendly — nil-able shapes,
  bounded lengths, hex-string fields with `As[Bytes]()` adapters
  at the parse boundary so the SSZ swap is mechanical, not
  semantic.

## References

- [Consensus-specs phase0/beacon-chain.md](https://github.com/ethereum/consensus-specs/blob/master/specs/phase0/beacon-chain.md) — canonical container declaration format
- [Consensus-specs ssz/simple-serialize.md](https://github.com/ethereum/consensus-specs/blob/master/ssz/simple-serialize.md) — primitive catalog
- [EIP-6404](https://eips.ethereum.org/EIPS/eip-6404) — SSZ transactions; map-as-list precedent; single-format-per-channel rule
- [EIP-6466](https://eips.ethereum.org/EIPS/eip-6466) — SSZ receipts
- [EIP-6465](https://eips.ethereum.org/EIPS/eip-6465) — SSZ withdrawals root; hash-tree-root as canonical identifier
- [EIP-7688](https://eips.ethereum.org/EIPS/eip-7688) — ProgressiveContainer for forward-compatible schemas
- [[chain-ssz-consensus-component-endpoint-2026-05-23]] — the broader endpoint reasoning
- [[manifest-driven-not-convention-driven-2026-05-23]] — the deeper anchor that this is one realization of
