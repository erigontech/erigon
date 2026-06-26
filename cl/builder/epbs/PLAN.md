# Erigon ePBS Builder

> **Last updated**: 2026-06-26 (spec sync against `ethereum/consensus-specs` `v1.7.0-alpha.10` `specs/gloas/`)
> **Previous**: 2026-05-26, 2026-04-22 (initial implementation notes)

Erigon-native ePBS block builder. Adds `--builder` flag to Erigon, letting node operators participate as builders with zero additional software.

## Motivation

ePBS (EIP-7732, Glamsterdam) makes builders first-class protocol participants. Unlike MEV-Boost era where builders needed relay relationships, ePBS allows anyone to deposit and start bidding. But there's no client with built-in builder support yet.

Erigon's all-in-one architecture (EL + Caplin CL in one process) gives a unique advantage: block building and bid submission share memory, eliminating Engine API / Beacon API HTTP overhead.

## Target Users

1. **Node operators already running Erigon** -- add `--builder` flag, zero extra cost
2. **Staking services** (Lido, Rocket Pool node operators) -- additional revenue stream
3. **Developers** -- experiment with builder role on devnet/testnet

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Erigon Process                             │
│                                                                     │
│  ┌─── CL side (cl/) ──────────────────────────────────────────┐    │
│  │                                                            │    │
│  │  Caplin slot ticker ──→ BuilderLoop       (cl/builder/epbs/)│   │
│  │                          │                                 │    │
│  │  ProposerPrefs gossip ──→│                                 │    │
│  │                          │                                 │    │
│  │                          ├─ waitForPreferences             │    │
│  │                          ├─ buildBlock() ◀───┐             │    │
│  │                          ├─ calcValue()      │             │    │
│  │                          ├─ decideBid()      │ (EL call)   │    │
│  │                          ├─ signBid()        │             │    │
│  │                          ├─ submitBid() ────────→ EpbsPool │    │
│  │                          │                   │   + gossip  │    │
│  │                          └─ revealPayload() ──→ gossip     │    │
│  │                                               │            │    │
│  │  BuilderManager (cl/builder/epbs/)            │            │    │
│  │  ├─ BLS key + DomainBeaconBuilder signing     │            │    │
│  │  ├─ deposit / exit / top-up                   │            │    │
│  │  └─ balance monitoring (from beacon state)    │            │    │
│  │                                               │            │    │
│  │  on_block handler ──→ detect own builderIndex │            │    │
│  │                       in committed bid        │            │    │
│  │                       → OnBidWon callback     │            │    │
│  └───────────────────────────────────────────────┼────────────┘    │
│                                                  │                 │
│  ┌─── EL side (execution/) ──────────────────────┼────────────┐    │
│  │                                               ▼            │    │
│  │  EL block-build stack (existing, layered):                 │    │
│  │                                                            │    │
│  │  INPUT side (kick off build):                              │    │
│  │   (L1-in) execution/builder/Builder.Build(params)          │    │
│  │            ← takes *builder.Parameters (has GasLimit after │    │
│  │              §1b extension)                                │    │
│  │   (L2-in) execmodule.AssembleBlock(ctx,                    │    │
│  │              *builder.Parameters)                          │    │
│  │            ← direct struct; add GasLimit *uint64 field     │    │
│  │   (L3-in) ChainReaderWriterEth1.AssembleBlock(hash,        │    │
│  │             *engine_types.PayloadAttributes)               │    │
│  │            ← Spec now has TargetGasLimit in PayloadAttrs;  │    │
│  │              update Erigon's PayloadAttributes to match.   │    │
│  │                                                            │    │
│  │  OUTPUT side (fetch assembled block):                      │    │
│  │   (L2-out) execmodule.GetAssembledBlock(id)                │    │
│  │             → {Block, BlockValue}  (execmodule.blockValue) │    │
│  │   (L3-out) ChainReaderWriterEth1.GetAssembledBlock(id)     │    │
│  │             → (*Eth1Block, *BlobsBundle,                   │    │
│  │                *RequestsBundle, *big.Int, error)           │    │
│  │   (L4-out) ExecutionClientDirect.GetAssembledBlock         │    │
│  │                                                            │    │
│  │  ePBS builder path:                                        │    │
│  │    • INPUT: call L3-in (PayloadAttributes now has           │    │
│  │      TargetGasLimit per spec; no L2 bypass needed).        │    │
│  │    • OUTPUT: call L3-out / L4-out to reuse the existing    │    │
│  │      Eth1Block / BlobsBundle / RequestsBundle conversion.  │    │
│  └────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘

Dependency direction: cl/builder/epbs → execution/builder (one-way, respects
existing architecture). Only CL knows about bids, slots, BLS, gossip.
EL stays agnostic — it just builds blocks on request.

**Consumption point**:
- **Input**: ePBS BuilderLoop calls `ChainReaderWriterEth1.AssembleBlock`
  (L3-in) with `PayloadAttributes` including `TargetGasLimit`. The spec
  now has `target_gas_limit` in `PayloadAttributes`, so the L3 bypass
  originally planned in §1b is no longer needed. Erigon's
  `engine_types.PayloadAttributes` must be extended to include
  `TargetGasLimit` and `SlotNumber` (see §1b TODO).
- **Output**: BuilderLoop calls `ChainReaderWriterEth1.GetAssembledBlock(id)`
  (L3-out) or `ExecutionClientDirect.GetAssembledBlock` (L4-out) to obtain
  the full CL-ready 5-tuple. No attribute is passed on this side, so the
  layer change does not re-introduce the `PayloadAttributes` gap.
- `execmodule.blockValue` (L2) is reused — **no separate
  `CalculateBlockValue` utility is created**.
```

## Existing Components (no changes needed)

Components already present, on which the builder relies:

| Component | Location | Branch | What it does |
|-----------|----------|--------|-------------|
| TxPool | `txnprovider/txpool/` | main | Stores pending txns, provides best txns by gas price |
| Block Builder | `execution/builder/` | main | Async block assembly: `Builder.Build()` returns full block + receipts. **Requires Phase 1 extension**: `Parameters.GasLimit *uint64` for per-build gas limit override (see "EL-side API extension" below). |
| EVM | `execution/vm/` | main | Transaction execution |
| State Reader | `execution/state/` | main | PlainState flat KV reads (O(1) lookup) |
| MEV-Boost client | `cl/beacon/builder/` | main | External relay builder client (reference — different code path) |
| ePBS Bid API | `cl/beacon/handler/epbs.go` | `feature/caplin_gloas` | POST/GET bid, payload attestation, envelope endpoints |
| ePBS Pool | `cl/pool/epbs_pool.go` | `feature/caplin_gloas` | LRU caches: `ProposerPreferences`, `HighestBids`, `PayloadAttestations` |
| P2P Gossip | `cl/gossip/gossip.go` | `feature/caplin_gloas` | Topics: `execution_payload` (envelope), `execution_payload_bid`, `proposer_preferences`, `payload_attestation_message` |
| Bid Validation | `cl/phase1/network/services/execution_payload_bid_service.go` | `feature/caplin_gloas` | Signature, timing, preference, balance checks |
| Builder registry | `cl/phase1/core/state/epbs.go` | `feature/caplin_gloas` | `ApplyDepositForBuilder`, `IsActiveBuilder`, `CanBuilderCoverBid` |
| ePBS types | `cl/cltypes/epbs_builder.go`, `epbs_payload.go`, `epbs_proposer.go` | `feature/caplin_gloas` | `Builder`, `ExecutionPayloadBid`, `ExecutionPayloadEnvelope`, `ProposerPreferences` |

## GLOAS State Model (single unified BeaconState)

GLOAS uses a **single unified `BeaconState`** — all builder fields are stored
directly in the beacon state struct (`cl/phase1/core/state/raw/state.go`),
not in a separate "payload state" or "builder state". Key design points:

- **State field layout (SSZ position 24)**: At GLOAS fork, position 24
  (`latestExecutionPayloadHeader`) is replaced by **`latestBlockHash`** (32 bytes),
  NOT `latestExecutionPayloadBid`. The bid is stored in the GLOAS extension
  fields at the end of the state. `LatestExecutionPayloadHeader` is no longer
  updated in GLOAS.
- **`latestBlockHash`** tracks the latest EL block hash. The builder MUST use
  `baseState.GetLatestBlockHash()` (not `LatestExecutionPayloadHeader().BlockHash`)
  to obtain the parent EL hash. See `block_production.go:642-678` for the
  reference pattern (which now branches on FULL/EMPTY — see below).
- **Deferred payload processing** (consensus-specs#5094): envelope processing
  (`OnExecutionPayload` / `ProcessExecutionPayloadEnvelope`) is **verification-only**
  — no state mutations occur when an envelope is received. Instead, the execution
  effects (deposits, withdrawals, consolidations, builder payments, `latest_block_hash`
  update) are applied at the **start of the next block's state transition** via
  `ProcessParentExecutionPayload`. This means:
  - There is no separate "post-envelope state" — only one state per block root.
  - `ForkGraph.GetExecutionPayloadState()` was removed.
  - When the builder's envelope is processed by the network, no balance changes
    or state mutations happen until the next block.
- Additional GLOAS state fields the builder interacts with:
  - `builders` (`ListSSZ[*Builder]`, cap `BuilderRegistryLimit = 1 << 40`)
  - `nextWithdrawalBuilderIndex` — builder withdrawal queue pointer
  - `executionPayloadAvailability` (`BitVector`) — PTC voting result on-chain
  - `builderPendingPayments` — in-flight payments from builders to proposers
  - `builderPendingWithdrawals` — queued builder balance withdrawals
  - `payloadExpectedWithdrawals` — expected withdrawals in the envelope
  - `latestExecutionPayloadBid` — cached in extension fields (not position 24)
  - `ptcWindow` — PTC committee assignments, processed by `ProcessPtcWindow` each epoch
- `BuilderIndexSelfBuild = math.MaxUint64` is a sentinel value for self-built
  blocks (no external builder). Self-builds require
  `Signature == bls.G2_POINT_AT_INFINITY` and `Value == 0`. Note: self-build
  envelope is signed by the **proposer's validator key**, not a builder key.
- `BuilderIndexFlag = 1 << 40` — builder indices have bit 40 set when used
  in validator-index contexts (withdrawal processing, exit). Use
  `ConvertBuilderIndexToValidatorIndex` / `ConvertValidatorIndexToBuilderIndex`
  for conversion.

### FULL vs EMPTY parent: EL parent hash determination

With deferred payload processing, the builder's speculative build must determine
the correct EL parent hash based on the parent block's payload status.

**Two related but distinct functions**:
- `should_extend_payload(store, root)` — fork-choice function; returns true if
  the payload at `root` is verified, timely, and data-available (or boost
  conditions are met). Used by PTC voting and fork choice weight.
- `should_build_on_full(store, head)` — proposer/builder function; returns true
  if `head.payload_status == FULL` and blob data is not voted unavailable.
  **This is what the builder should use** to decide the EL parent hash.

```
Parent was FULL (should_build_on_full = true):
  → EL parent hash = parentBid.BlockHash (the actual EL block)
  → Withdrawals: copy state → apply_parent_execution_payload(state,
    envelope.execution_requests) → get_expected_withdrawals(state).withdrawals
    (requires state mutation on the copy to compute correct withdrawal set)

Parent was EMPTY (should_build_on_full = false):
  → EL parent hash = parentBid.ParentBlockHash (grandparent's EL block)
  → Withdrawals: use cached state.payload_expected_withdrawals (no mutation needed)

No parent bid (pre-GLOAS or genesis):
  → EL parent hash = baseState.GetLatestBlockHash()
```

**Withdrawals must be exact.** Envelope verification checks:
```python
assert hash_tree_root(payload.withdrawals) == hash_tree_root(state.payload_expected_withdrawals)
```
Wrong withdrawals → envelope rejected by the network.

**`parent_execution_requests` short-circuit.** When the parent was EMPTY
(`bid.parent_block_hash != parent_bid.block_hash`),
`process_parent_execution_payload` asserts `requests == ExecutionRequests()`
and returns immediately — no deposits/withdrawals/consolidations are processed.
The builder's `execution_requests_root` in the bid must account for this.

Reference: `cl/beacon/handler/block_production.go` (`prepare_execution_payload`
function). The builder's `OnNewHead` must replicate this logic to start
speculative builds with the correct parent hash and withdrawals.

## Package Location & Dependency Direction

The ePBS builder lives under **`cl/builder/epbs/`**, not `execution/builder/`.

Rationale:
- Existing dependency direction is `cl/ → execution/` (single-directional). Only
  `execution/engineapi/` imports `cl/` because Engine API is an EL-exposed protocol API.
- The ePBS builder is fundamentally CL-driven: slot ticks, `ProposerPreferences`
  gossip, BLS signing with `DomainBeaconBuilder`, bid submission to Caplin's pool,
  envelope broadcast via CL gossip, bid-win detection via `on_block`.
- EL dependency is narrow: a single `execution/builder.Builder.Build()` call which
  already encapsulates TxPool access. No direct TxPool coupling needed.
- Placing it in `execution/builder/` would force `execution/ → cl/cltypes,
  cl/pool, cl/gossip, cl/utils/bls, cl/clparams, cl/fork`, reversing the
  architecture's dependency direction.
- Future needs (bundle API, multi-ordering simulation) are handled by extending
  `execution/builder/`'s API surface — the ePBS builder still just calls `Build()`.

No new EL-side utility is added. Block value is computed by the existing
`execmodule.blockValue` (`execution/execmodule/block_building.go:84`), which
`ChainReaderWriterEth1.GetAssembledBlock` already returns as a `*big.Int`.

## New Components

### 1. BuilderLoop -- slot-driven block building + bidding

> **Implementation note (2026-04-22)**: BuilderLoop is fully implemented in
> `cl/builder/epbs/loop.go` on `feature/epbs_builder`. The pseudocode below is the
> original design sketch. The actual struct uses `BuilderManager`, `BidStrategy`,
> `SpeculativeBuild`, `PreferencesWatcher`, `BidSubmitter` with `SlotContext`
> instead of `ParentInfo`. `OnBidWon` takes 4 args including `beaconBlockRoot`.

The core new logic. Listens for slot events and drives the build-bid-reveal cycle.

**Critical protocol constraints:**
- Builder MUST match `SignedProposerPreferences` at bid time — bids are
  `[REJECT]`ed if `fee_recipient` doesn't match. Gas limit (`target_gas_limit`
  in the spec) compatibility is `[IGNORE]` — bids with incompatible gas limits
  are silently dropped, not rejected. Builder should match `target_gas_limit`
  to avoid being ignored.
- Same slot can have **multiple independent bid markets** keyed by
  `(Slot, ParentBlockHash, ParentBlockRoot)`. After reorgs, builder may need to
  bid into multiple markets simultaneously.
- Bids are commitments: if bid wins but payload isn't revealed, builder's beacon
  state balance is deducted (value burned). **Never bid without a ready payload.**

**Speculative build is MVP, not an optimisation.** The naive "wait for
preferences → build → bid" sequence does not fit the 3000ms bid deadline: if
preferences arrive at 1500ms and EL build takes 500ms, only ~1000ms remain for
signing, pool validation, and gossip propagation. Even typical builds leave no
margin. BuilderLoop therefore starts EL block assembly **as soon as the parent
head is known** (potentially before slot start) using a provisional
fee_recipient/gas_limit; when preferences arrive, it either:
  - (fast path) preferences match provisional values → sign & submit the
    already-built block;
  - (rebuild path) preferences differ → issue a fresh FCU with updated
    PayloadAttributes; race the rebuild against the 3000ms deadline. If rebuild
    budget blown, skip this slot.

```go
// cl/builder/epbs/loop.go

type BuilderLoop struct {
    blockBuilder   BlockBuilderFunc      // existing EL block builder
    bidSubmitter   BidSubmitter          // interface to Caplin (direct call, no HTTP)
    bidStrategy    BidStrategy
    builderManager *BuilderManager
    forkChoice     ForkChoiceReader      // read current heads / parent candidates
    prefsWatcher   PreferencesWatcher    // monitors ProposerPreferences gossip
    headWatcher    HeadWatcher           // notifies on fork choice head changes (speculative trigger)
    logger         log.Logger

    // In-flight speculative builds keyed by BidMarketKey. Started before
    // preferences arrive; finalised (or rebuilt) once preferences land.
    speculative sync.Map  // BidMarketKey → *SpeculativeBuild

    // Payload store: keep built blocks for reveal after bid wins.
    // Key: (slot, parentBlockHash, parentBlockRoot)
    pendingPayloads sync.Map
}

type SpeculativeBuild struct {
    ParentHash    common.Hash
    ParentRoot    common.Hash
    PayloadID     uint64
    StartedAt     time.Time
    ProvisionalFR common.Address  // our fallback fee_recipient used pre-prefs
    ProvisionalGL uint64          // our fallback gas_limit used pre-prefs
    Cancel        context.CancelFunc
}

// OnNewHead is called when fork choice establishes a new viable parent for
// the upcoming slot. Starts EL build immediately with provisional attributes.
// This is the speculative trigger: we don't wait for preferences.
//
// GLOAS FULL/EMPTY branching: the EL parent hash depends on the parent
// block's payload status (see "FULL vs EMPTY parent" section above):
//   FULL:    parent.Hash = parentBid.BlockHash
//   EMPTY:   parent.Hash = parentBid.ParentBlockHash (grandparent)
//   No bid:  parent.Hash = baseState.GetLatestBlockHash()
// The caller (HeadWatcher) must resolve this before invoking OnNewHead.
func (b *BuilderLoop) OnNewHead(slot uint64, parent ParentInfo) {
    key := BidMarketKey{slot, parent.Hash, parent.Root}
    if _, exists := b.speculative.Load(key); exists {
        return
    }
    ctx, cancel := context.WithTimeout(b.ctx, 2500*time.Millisecond)
    spec := &SpeculativeBuild{
        ParentHash: parent.Hash, ParentRoot: parent.Root,
        StartedAt: time.Now(),
        ProvisionalFR: b.builderManager.Pubkey().Address(), // our own addr
        ProvisionalGL: parent.GasLimit,                      // inherit parent
        Cancel: cancel,
    }
    b.speculative.Store(key, spec)
    go b.startEngineBuild(ctx, spec, slot, parent)
}

// OnSlot runs the bid finalisation phase. By now speculative builds should be
// in flight or done; we just need preferences to match/rebuild.
func (b *BuilderLoop) OnSlot(slot uint64) {
    // 1. Wait for ProposerPreferences — but tighter timeout because we've
    //    already started building speculatively. Timeout 2500ms leaves 500ms
    //    for sign+submit before 3000ms deadline.
    prefs, err := b.prefsWatcher.WaitForPreferences(slot, 2500*time.Millisecond)
    if err != nil {
        b.logger.Debug("[builder] no preferences for slot, skipping", "slot", slot)
        return
    }

    // 2. Enumerate active bid markets from fork choice.
    //    Normally one parent, but reorgs can create multiple.
    parents := b.forkChoice.ActiveParents(slot)

    for _, parent := range parents {
        b.buildAndBid(slot, parent, prefs)
    }
}

func (b *BuilderLoop) buildAndBid(slot uint64, parent ParentInfo, prefs *cltypes.ProposerPreferences) {
    // 3. Check if speculative build matches preferences.
    key := BidMarketKey{slot, parent.Hash, parent.Root}
    var block *types.Block
    var receipts types.Receipts

    if specAny, ok := b.speculative.Load(key); ok {
        spec := specAny.(*SpeculativeBuild)
        if spec.ProvisionalFR == prefs.FeeRecipient && spec.ProvisionalGL == prefs.GasLimit {
            // Fast path: speculative build matches; just collect it.
            block, receipts = b.collectSpeculativeBuild(spec)
        } else {
            // Rebuild path: preferences differ; cancel old build, issue new FCU
            // with updated PayloadAttributes, race against 3000ms deadline.
            spec.Cancel()
            block, receipts = b.rebuildBlock(slot, parent, prefs.FeeRecipient, prefs.GasLimit)
            if block == nil {
                b.logger.Warn("[builder] rebuild budget blown, skipping slot", "slot", slot)
                return
            }
        }
    } else {
        // No speculative build (e.g., missed OnNewHead). Fall back to synchronous.
        block, receipts = b.buildBlock(slot, parent.Hash, prefs.FeeRecipient, prefs.GasLimit)
    }

    // 4. Calculate block value (sum of tx priority fees).
    blockValue := calculateBlockValue(block, receipts)

    // 5. Decide bid amount.
    bidAmount := b.bidStrategy.Decide(slot, blockValue)
    if bidAmount == nil {
        return // not profitable enough
    }

    // 6. Create and sign bid (DomainBeaconBuilder 0x0B000000, GLOAS fork version).
    bid := &cltypes.ExecutionPayloadBid{
        Slot:                  slot,
        BuilderIndex:          b.builderManager.BuilderIndex(),
        ParentBlockHash:       parent.Hash,
        ParentBlockRoot:       parent.Root,
        BlockHash:             block.Hash(),
        PrevRandao:            block.PrevRandao(),  // must match state randao mix
        FeeRecipient:          prefs.FeeRecipient,
        GasLimit:              prefs.GasLimit,
        Value:                 bidAmount,
        ExecutionPayment:      0, // spec: must be 0
        BlobKzgCommitments:    extractBlobCommitments(block), // from blob txns
        ExecutionRequestsRoot: computeRequestsRoot(requests), // hash_tree_root(execution_requests)
    }
    signedBid := b.builderManager.SignBid(bid)

    // 7. Submit to Caplin (direct function call, same process).
    b.bidSubmitter.SubmitBid(signedBid)

    // 8. Store block for later reveal — keyed by bid market.
    key := BidMarketKey{slot, parent.Hash, parent.Root}
    b.pendingPayloads.Store(key, &PendingPayload{Block: block, Receipts: receipts})
}

// OnBidWon is called when the proposer commits to our bid in their beacon block.
// Builder MUST reveal the payload before the PTC attestation deadline (75% of slot = 9000ms).
// Target: broadcast by ~6500ms to allow ~2500ms network propagation to PTC members.
// beaconBlockRoot is needed to construct ExecutionPayloadEnvelope.BeaconBlockRoot.
func (b *BuilderLoop) OnBidWon(ctx context.Context, slot uint64, parentHash common.Hash, parentRoot common.Hash, beaconBlockRoot common.Hash) {
    key := BidMarketKey{slot, parentHash, parentRoot}
    val, ok := b.pendingPayloads.LoadAndDelete(key)
    if !ok {
        b.logger.Error("[builder] bid won but no stored payload!", "slot", slot)
        return // catastrophic: we'll lose our bid value
    }
    pending := val.(*PendingPayload)

    envelope := b.buildEnvelope(slot, pending.Block)
    signedEnvelope := b.builderManager.SignEnvelope(envelope)
    b.bidSubmitter.BroadcastPayload(signedEnvelope)

    b.logger.Info("[builder] payload revealed", "slot", slot)
}
```

**`ExecutionPayloadEnvelope` struct** (built by `buildEnvelope`):
```go
// cl/cltypes/epbs_payload.go
type ExecutionPayloadEnvelope struct {
    Payload               *Eth1Block         `json:"payload"`
    ExecutionRequests     *ExecutionRequests  `json:"execution_requests"`
    BuilderIndex          uint64             `json:"builder_index,string"`
    BeaconBlockRoot       common.Hash        `json:"beacon_block_root"`
    ParentBeaconBlockRoot common.Hash        `json:"parent_beacon_block_root"`
}
```

Only 5 exported fields — `Slot` and `StateRoot` do NOT exist on the envelope
struct. `Slot` is inferred from `Payload.SlotNumber`. `BeaconBlockRoot` is the
root of the beacon block that committed the winning bid. `ParentBeaconBlockRoot`
is the parent beacon block root, required by the EL for EIP-4788 beacon root
storage (`process_beacon_block_root`) and validated during envelope processing
(`ProcessExecutionPayloadEnvelope` in `operations.go`).

**Must use constructor**: `NewExecutionPayloadEnvelope(cfg)` — the unexported
`beaconCfg` field is needed for SSZ encoding/decoding. Do not use struct literal.

**`ProposerPreferences` struct** (from p2p-interface spec):
```python
class ProposerPreferences(Container):
    dependent_root: Root              # fork-dependent; ensures prefs match the
                                      # expected proposer shuffling
    proposal_slot: Slot
    validator_index: ValidatorIndex
    fee_recipient: ExecutionAddress
    target_gas_limit: uint64          # NOTE: not "gas_limit"
```

The builder matches `fee_recipient` and `target_gas_limit` from the
proposer's preferences. If no `SignedProposerPreferences` is broadcast
for a slot, the proposer will not accept any trustless bid for that slot.

**`BeaconBlockBody` changes in GLOAS** (from beacon-chain spec):

Removed fields:
- `execution_payload`
- `blob_kzg_commitments`
- `execution_requests`

Added fields:
- `signed_execution_payload_bid: SignedExecutionPayloadBid`
- `payload_attestations: List[PayloadAttestation, MAX_PAYLOAD_ATTESTATIONS]` (max 4)
- `parent_execution_requests: ExecutionRequests`

**Broadcast pattern**: After constructing and signing the envelope, the builder
must BOTH gossip-publish it AND process it locally via `OnExecutionPayload` so
the local node transitions the block from PENDING to FULL. See
`block_production.go:broadcastSelfBuildEnvelope` for the reference pattern.

### 1b. EL-side API extension (required prerequisite)

The existing `execution/builder.Builder.Build(params)` cannot be used as-is.
Two gaps must be filled before BuilderLoop can function:

**Gap 1: per-build `GasLimit`.** `execution/builder/parameters.go:26-35` today
carries only `PayloadId / ParentHash / Timestamp / PrevRandao /
SuggestedFeeRecipient / Withdrawals / ParentBeaconBlockRoot / SlotNumber`. Gas
limit is pulled from `BuilderConfig.GasLimit` inside
`create_block.go:88` (`MakeEmptyHeader(parent, cfg.chainConfig, timestamp,
cfg.builder.BuilderConfig.GasLimit)`), i.e. a **process-wide config value**.
ePBS validation REJECTs any bid whose `gas_limit` differs from the proposer's
preferences (strict equality in Caplin; the Gloas spec uses
`is_gas_limit_target_compatible()` with `[IGNORE]` semantics — TODO: align),
so the builder must be able to target a specific gas limit *per build*.
Required change:

```go
// execution/builder/parameters.go
type Parameters struct {
    PayloadId             uint64
    ParentHash            common.Hash
    Timestamp             uint64
    PrevRandao            common.Hash
    SuggestedFeeRecipient common.Address
    GasLimit              *uint64   // NEW — optional per-build override
    Withdrawals           []*types.Withdrawal
    ParentBeaconBlockRoot *common.Hash
    SlotNumber            *uint64
}

// execution/builder/create_block.go:88
gasLimit := cfg.builder.BuilderConfig.GasLimit
if p := cfg.blockBuilderParameters; p != nil && p.GasLimit != nil {
    gasLimit = *p.GasLimit
}
header := MakeEmptyHeader(parent, cfg.chainConfig, timestamp, gasLimit)
```

Backward-compatible: when `GasLimit` is nil, existing callers get current
behaviour. Caplin's existing `block_production.go` path passes nil; ePBS
builder passes `&prefs.GasLimit`.

**`PayloadAttributes` now has `target_gas_limit`.** The Gloas fork-choice
spec added `target_gas_limit` and `slot_number` to `PayloadAttributes`:

```python
@dataclass
class PayloadAttributes:
    timestamp: uint64
    prev_randao: Bytes32
    suggested_fee_recipient: ExecutionAddress
    withdrawals: Sequence[Withdrawal]
    parent_beacon_block_root: Root
    slot_number: uint64          # NEW in Gloas
    target_gas_limit: uint64     # NEW in Gloas
```

This means the original L3-bypass rationale ("`PayloadAttributes` has no
`GasLimit` field") **no longer holds**. Erigon's internal
`engine_types.PayloadAttributes` and `ChainReaderWriterEth1.AssembleBlock`
should be extended to carry `target_gas_limit`, keeping the ePBS builder
on the same code path as Caplin's proposer. The `builder.Parameters`
extension above is still needed (it's the struct that reaches
`create_block.go`), but the L3 bypass is no longer required — the ePBS
builder can go through the standard `AssembleBlock(hash, PayloadAttributes)`
entrypoint once `PayloadAttributes` propagates gas limit.

**TODO**: update `execution/engineapi/engine_types/jsonrpc.go` to add
`TargetGasLimit` and `SlotNumber` to `PayloadAttributes`, and wire them
through `ChainReaderWriterEth1.AssembleBlock` → `builder.Parameters`.

**Gap 2: rebuild / restart support.** Speculative builds need to be cancelled
and re-issued when preferences differ from the provisional fee_recipient/
gas_limit. Existing `Builder.Build()` is a single call; we need either:
- A `Cancel()` API on an in-flight build so the goroutine exits promptly and
  releases the SharedDomains snapshot, *or*
- A no-op if we just issue a second `FCU` with a different `PayloadId` — EL
  handles them independently, first one's goroutine eventually times out via
  `interrupt *atomic.Bool`. This is the lower-touch option but wastes CPU on
  the abandoned build.

MVP: take the second option (multiple concurrent builds, cheapest to ship).
Phase 2: add explicit `Cancel()` if measurements show CPU contention during
rebuild storms on reorg-heavy slots.

**Semaphore risk**: `ExecModule.AssembleBlock` uses `e.semaphore.TryAcquire(1)`.
If the semaphore size is 1, a second concurrent `AssembleBlock` call returns
`Busy: true`. The speculative build must call `GetAssembledBlock` (which releases
resources) or complete before issuing a rebuild. Verify semaphore capacity during
implementation.

**FCU dependency**: `ExecModule.AssembleBlock` builds blocks against the EL's
current head state. Caplin already sends Fork Choice Updates on head changes
(`block_production.go:776-790`). The builder's speculative trigger (`OnNewHead`)
must wait for the normal FCU to be processed by the EL before calling
`AssembleBlock` — otherwise the EL may build against a stale parent. For MVP,
relying on Caplin's existing FCU timing is sufficient.

### Reveal-path latency budget

After Codex's review, the reveal path is tight enough to need an explicit
budget. BidWon detection must move *earlier* than the originally-planned
`forkchoice.OnBlock` hook (see Partition B revision below).

| Stage | Typical | Worst | Cumulative worst |
|-------|---------|-------|------------------|
| Proposer publishes block (slot 0 baseline) | ~3500ms | ~4000ms | 4000ms |
| Gossip propagation to our node | 50ms | 300ms | 4300ms |
| Block parse + proposer signature verify | 5ms | 20ms | 4320ms |
| **BidWon emit** (early path — pre-state-transition) | 1ms | 5ms | **4325ms** |
| Envelope construction (cached payload) | 5ms | 30ms | 4355ms |
| Envelope sign (LocalSigner) | 2ms | 10ms | 4365ms |
| Gossip publish (local sentinel) | 5ms | 20ms | 4385ms |
| Network propagation to PTC members | 500ms | 2000ms | **6385ms** ✓ |
| PTC deadline (9000ms) — margin | | | **2615ms** |

Contrast with original PLAN's **late emit** (inside `forkchoice.OnBlock`
after state transition + payload validation): adds 100–500ms, cumulative worst
jumps to ~4800ms, shrinking our propagation budget from 2615ms to ~2000ms on
bad days. Not fatal, but avoidable.

### 2. BuilderManager -- builder identity and lifecycle

> **Implementation note (2026-04-22)**: BuilderManager is implemented in
> `cl/builder/epbs/manager.go`. Key operations: `SignBid`, `SignEnvelope`,
> `ResolveIndex(sd synced_data.SyncedData)`. The `Signer` interface and
> `LocalSigner` are in `signer.go` / `signer_local.go`.

Builder must be registered in the beacon state before it can bid. Registration
requires a deposit; the builder's `builderIndex` is assigned on-chain.

**Signing domains**: Bid and envelope signatures use `DomainBeaconBuilder`
(`0x0B000000`). Builder deposit signatures use `DomainBuilderDeposit`
(`0x0E000000`) — distinct from the validator `DomainDeposit` (`0x03000000`).
Legacy deposits made before the GLOAS fork (onboarded via
`onboard_builders_from_pending_deposits`) still verify with `DomainDeposit`;
`IsValidDepositSignature` tries both domains.

**Balance constraint**: Bid `Value` is deducted from builder's beacon state balance
when the bid is committed. If the builder doesn't reveal, the value is still
deducted (penalty for non-delivery). The `BuilderManager` must track balance and
refuse to bid if balance is too low.

**Signer abstraction**: `BuilderManager` does not hold the BLS key directly.
Instead it delegates signing to a `Signer` interface. Phase 1 ships with an
in-process local signer (key loaded from disk) — same posture as Nimbus's and
Teku's embedded validator modes, which have run in mainnet production for years
without incident. Phase 3 adds an optional remote-signer adapter
(Web3Signer-compatible) for operators whose threat model demands it; Phase 2's
hot/cold separation and circuit breaker are higher-impact for bounding actual
loss (see "Key-security model" under Risks).

```go
// cl/builder/epbs/signer.go

type Signer interface {
    // Pubkey is used for ResolveIndex and stable identity across signer swaps.
    Pubkey() common.Bytes48

    // SignBid signs an ExecutionPayloadBid with DomainBeaconBuilder.
    // Domain computation is done by the caller so remote signers never need
    // fork/genesis context — they just sign the supplied signing_root.
    SignBid(ctx context.Context, signingRoot common.Hash) (common.Bytes96, error)

    // SignEnvelope signs an ExecutionPayloadEnvelope with DomainBeaconBuilder.
    SignEnvelope(ctx context.Context, signingRoot common.Hash) (common.Bytes96, error)

    // SignDeposit signs a DepositMessage with DomainBuilderDeposit (0x0E000000).
    // Only called during builder bootstrap; remote signers must support this
    // domain even though it's used rarely.
    SignDeposit(ctx context.Context, signingRoot common.Hash) (common.Bytes96, error)
}

// cl/builder/epbs/signer_local.go — Phase 1 default
type LocalSigner struct {
    sk     bls.SecretKey
    pubkey common.Bytes48
}

func NewLocalSignerFromFile(path string) (*LocalSigner, error) { ... }

// cl/builder/epbs/signer_web3.go — Phase 2
type Web3Signer struct {
    url        string           // e.g. http://web3signer:9000
    pubkey     common.Bytes48   // which key on the remote signer to use
    httpClient *http.Client
    // Uses /api/v1/eth2/sign/{pubkey} with { type, signingRoot, forkInfo } body.
    // Web3Signer understands the BEACON_BUILDER signing type natively once its
    // GLOAS support ships; until then we send raw `signing_root` with type=DATA.
}

func NewWeb3Signer(url string, pubkey common.Bytes48) *Web3Signer { ... }
```

```go
// cl/builder/epbs/manager.go

type BuilderManager struct {
    signer       Signer               // LocalSigner or Web3Signer
    builderIndex uint64
    depositBalance *big.Int

    // Signing context — used to compute domain, not passed to remote signer
    cfg                   *clparams.BeaconChainConfig
    genesisValidatorsRoot common.Hash
}

// Key operations:
func (m *BuilderManager) SignBid(ctx context.Context, bid *cltypes.ExecutionPayloadBid) (*cltypes.SignedExecutionPayloadBid, error)
func (m *BuilderManager) SignEnvelope(ctx context.Context, env *cltypes.ExecutionPayloadEnvelope) (*cltypes.SignedExecutionPayloadEnvelope, error)
func (m *BuilderManager) Pubkey() common.Bytes48
func (m *BuilderManager) BuilderIndex() uint64
func (m *BuilderManager) IsActive() bool
func (m *BuilderManager) CanCoverBid(value uint64) bool  // balance check

// Lifecycle (required for Phase 1 on devnet):
func (m *BuilderManager) Deposit(ctx context.Context, amount *big.Int) error
func (m *BuilderManager) ResolveIndex(beaconAPI string) error  // pubkey → on-chain index
func (m *BuilderManager) MonitorBalance()  // periodic beacon state query

// Lifecycle (Phase 2):
func (m *BuilderManager) Exit(ctx context.Context) error
func (m *BuilderManager) TopUp(ctx context.Context, amount *big.Int) error
```

**Signing-root computation**: the manager computes `signing_root = hash_tree_root(SigningData{object_root, domain})` locally and passes it to the signer. This keeps the signer interface minimal (just sign 32 bytes) and means the remote signer never needs to know fork schedule or genesis root — matching how Web3Signer handles validator attestations today.

**State access pattern**: `BuilderManager` should receive a `synced_data.SyncedData`
reference (same pattern as bid validation service). Use
`syncedData.ViewHeadState(func(s *state.CachingBeaconState) error { ... })` for:
- `ResolveIndex`: iterate `state.GetBuilders()` to find pubkey match (O(n), cache result)
- `MonitorBalance`: read `state.GetBuilders().Get(builderIndex).Balance`
- `CanCoverBid`: call existing `state.CanBuilderCoverBid(s, builderIndex, amount)`
- `IsActive`: call existing `state.IsActiveBuilder(s, builderIndex)`

### 3. BidStrategy -- how much to bid

```go
// cl/builder/epbs/strategy.go

type BidStrategy interface {
    Decide(slot uint64, blockValue *big.Int) *big.Int
}

// MVP: fixed margin
type FixedMarginStrategy struct {
    Margin    float64   // 0.85 = keep 15% profit
    MinProfit *big.Int  // don't bid if profit below this
}

func (s *FixedMarginStrategy) Decide(slot uint64, blockValue *big.Int) *big.Int {
    bid := new(big.Int).Mul(blockValue, big.NewInt(int64(s.Margin * 100)))
    bid.Div(bid, big.NewInt(100))
    profit := new(big.Int).Sub(blockValue, bid)
    if profit.Cmp(s.MinProfit) < 0 {
        return nil // skip this slot
    }
    return bid
}
```

### 4. CLI Flags

```go
// cmd/erigon/main.go (new flags)

// Phase 1:
--builder                         // enable builder mode
--builder.key <path>              // BLS secret key file (local signer)
--builder.fee-recipient <addr>    // where to receive payments
--builder.bid-margin <float>      // default: 0.85
--builder.min-profit <wei>        // default: 0 (bid on everything)
--builder.bid-strategy <name>     // default: "fixed" (only option in MVP)

// Phase 2 (financial safety):
--builder.max-hot-balance <wei>   // cap hot balance; sweep excess to cold
--builder.cold-address <addr>     // destination for profit sweeps
--builder.reserve-multiplier <n>  // min hot = max_bid × n (default 10)
--builder.circuit-breaker <n>     // pause after N consecutive non-reveals (default 3)

// Phase 3 (optional remote signer):
--builder.remote-signer <url>     // Web3Signer URL; mutually exclusive with --builder.key
--builder.pubkey <hex>            // required with --builder.remote-signer; 48-byte BLS pubkey
```

**Key security**: `--builder.key` loads the BLS key into the Erigon process.
This follows the Nimbus/Teku embedded-VC precedent and is **acceptably safe**
given builder keys are non-slashable and Go is memory-safe. The most effective
mitigations are `--builder.max-hot-balance` + `--builder.circuit-breaker`
(Phase 2), not process separation. `--builder.remote-signer` (Phase 3) is
useful only when the signer runs on a different host with smaller attack
surface than Erigon — same-host remote signing provides negligible benefit.
See "Key-security model" under Risks.

### 5. BidSubmitter -- interface between EL builder and CL

```go
// cl/builder/epbs/submitter.go

type BidSubmitter interface {
    SubmitBid(bid *cltypes.SignedExecutionPayloadBid) error
    // BroadcastPayload must BOTH:
    // 1. Gossip-publish the envelope on TopicNameExecutionPayload
    // 2. Process it locally via OnExecutionPayload so our node
    //    transitions the block from PENDING to FULL
    // See block_production.go:broadcastSelfBuildEnvelope for reference.
    BroadcastPayload(envelope *cltypes.SignedExecutionPayloadEnvelope) error
}

// Direct implementation: calls Caplin functions in-process
type CaplinBidSubmitter struct {
    bidService      services.ExecutionPayloadBidService
    gossipManager   *network.GossipManager
    epbsPool        *pool.EpbsPool
}
```

## Block Value Calculation

**Already exists — do not re-implement.** `execmodule.blockValue`
(`execution/execmodule/block_building.go:84-122`) computes
`sum(GasUsed × GetEffectiveGasTip)` from a `BlockWithReceipts` + baseFee and
returns a `*uint256.Int`. `ChainReaderWriterEth1.GetAssembledBlock`
(`execution/execmodule/chainreader/chain_reader.go:478`) already surfaces it
as the 4th return value (`*big.Int`).

```go
// From execution/execmodule/block_building.go (existing):
func blockValue(br *types.BlockWithReceipts, baseFee *uint256.Int) *uint256.Int {
    v := uint256.NewInt(0)
    txs := br.Block.Transactions()
    for i := range txs {
        gas := new(uint256.Int).SetUint64(br.Receipts[i].GasUsed)
        effectiveTip := txs[i].GetEffectiveGasTip(baseFee)
        v.Add(v, new(uint256.Int).Mul(gas, effectiveTip))
    }
    return v
}
```

ePBS `BidStrategy` consumes the `*big.Int` value returned by
`ChainReaderWriterEth1.GetAssembledBlock`. No new utility file is added.

## Slot Timeline

ePBS uses a BPS (Basis Points) system: 10000 BPS = 1 slot (12s on mainnet).
Deadlines are protocol-enforced via fork choice timeliness checks.

**GLOAS timing changes from pre-GLOAS forks:**
- `ATTESTATION_DUE_BPS_GLOAS = 2500` (25%, was 33% pre-GLOAS)
- `AGGREGATE_DUE_BPS_GLOAS = 5000` (50%, was 66%)
- `PAYLOAD_DUE_BPS = 7500` (75%, new — envelope must arrive before this)
- `PAYLOAD_ATTESTATION_DUE_BPS = 7500` (75%, new — PTC votes due)

**Note**: there is no explicit "bid deadline" constant in the spec. The
effective deadline is before the proposer publishes their block (the proposer
selects from received bids). The 3000ms figure below is the attestation
deadline, which is when the proposer typically finalises the block.

```
New-head     0ms        ~1500ms    3000ms              6500ms   9000ms        12000ms
 event       |──────────|──────────|───────────────────|────────|──────────────|
  │          Slot start   Prefs     Bid deadline        Reveal   PTC deadline   Slot end
  │                       arrive    (attestation        target   (75% = 9000ms)
  ▼                       (typical  due = 25%)          ↑
 ─────────── speculative build in flight ───────          │
     (started on OnNewHead, uses provisional            Must broadcast by here
      FR/GL; finalised or rebuilt when prefs arrive)    to reach PTC in time
```

| Phase | BPS | Time (mainnet) | What happens |
|-------|-----|----------------|-------------|
| **OnNewHead** (speculative trigger) | pre-slot | −500 to 0ms | Fork-choice establishes parent for upcoming slot; BuilderLoop starts EL build with provisional fee_recipient (ours) + gas_limit (parent's). Typical EL build latency ~200-500ms runs concurrently with the preferences wait. |
| Preferences arrive | 0-1250 | 0-1500ms | Proposer gossips `SignedProposerPreferences`; builder compares with provisional attributes. |
| Finalise or rebuild | 1250-2500 | 1500-3000ms | **Fast path**: provisional matches → collect speculative block, sign + submit bid (~50ms). **Rebuild path**: preferences differ → issue fresh FCU with updated attributes, race against deadline. **Skip path**: rebuild budget blown → no bid this slot. |
| **Attestation deadline** | **2500** | **3000ms** | Bids must arrive before this; late bids lose proposer boost consideration. |
| Proposer commits | 2500-5000 | 3000-6000ms | Proposer selects highest bid, includes in beacon block. |
| **BidWon detection (early path)** | ~3300 | ~4000-4400ms | BeaconBlock arrives via gossip; after proposer-sig verify (before full state transition), emit BidWon. |
| **Reveal target** | **~5400** | **~6500ms** | Builder broadcasts `SignedExecutionPayloadEnvelope` — aim for 6500ms to allow 2500ms propagation. |
| **PTC deadline** | **7500** | **9000ms** | Payload Timeliness Committee votes; envelope must be received by PTC members before this. |
| PTC voting window | 7500-10000 | 9000-12000ms | PTC members submit `PayloadAttestationMessage` (payload_present, blob_data_available). |

**Critical**: if envelope arrives after 9000ms → PTC votes `payload_present=false`
→ PayloadStatus=Empty → builder's bid value is deducted but block is empty (pure loss).

**Penalty for non-reveal**: builder's beacon balance is reduced by bid `Value`.
This is not slashing (no validator ejection), but it's a direct financial loss.
The builder MUST have the payload ready before bidding — never bid speculatively
on a payload whose reveal is uncertain.

### Honest Payload Withholding Strategy

The builder should reveal only when the committed beacon block is timely and
still plausibly canonical. On `OnBidWon`, check the observed block arrival time
against the attestation deadline and confirm the block extends the current head
or an otherwise viable fork-choice head. If the block is late and no longer the
current head, withhold the envelope: revealing cannot help the canonical chain
and only leaks payload contents. If the block is timely or still the head, reveal
immediately; withholding a canonical payload turns the bid into a direct loss.

**Why speculative build is mandatory** (not an optimisation): the naïve
sequential "wait 1500ms for prefs → build 500ms → sign+submit 100ms → propagate
300ms" needs ≥2400ms of 3000ms, leaving no margin for preference arrival
jitter. Speculative build moves the ~500ms EL work off the critical path,
restoring ~1000ms of headroom for prefs lateness / rebuild / gossip.

## Implementation Plan

### Phase 1: MVP — Progress Tracker

**Status (2026-04-22)**: Core builder package (`cl/builder/epbs/`) is largely
implemented on `feature/epbs_builder` (21 Go files, including tests). Partition B
(all 4 CL hooks) and most of Partition C are done. Remaining work is the EL-side
`GasLimit` API gap, deposit bootstrap verification, and integration testing.

Goal: Erigon can run as builder on ePBS devnet, build blocks, submit bids, reveal payloads.

Tags:
- 🔴 **blocker** — nothing else can be integration-tested until this is done
- 🟢 **Partition C** — builder-specific code under `cl/builder/epbs/`
- 🟣 **Partition B** — CL hooks (all done)
- 🔵 **Partition A** — consumer of existing GLOAS APIs
- 🟠 **integration** — end-to-end, requires live CL stack

```
Identity + Signing + Deposit:
  🔴 [ ] **BLOCKER**: devnet builder-deposit bootstrap verification
         Verify EIP-8282 BuilderDepositRequest dispatches to
         ApplyDepositForBuilder (not validator pipeline). New deposits use
         PAYLOAD_BUILDER_VERSION (0x00) credentials and DOMAIN_BUILDER_DEPOSIT;
         BUILDER_WITHDRAWAL_PREFIX (0x03) is fork-onboarding-only. Confirm
         GLOAS fork active at target devnet. End-to-end smoke test.
  🟢 [x] Signer interface (Pubkey, SignBid, SignEnvelope, SignDeposit)          → signer.go
  🟢 [x] LocalSigner + BLS key file loading                                   → signer_local.go
  🟢 [x] BuilderManager (signing, ResolveIndex, identity)                      → manager.go
  🔵 [x] DomainBeaconBuilder signing-root computation                          → manager.go
  🔵 [x] Builder deposit flow (DepositData + PAYLOAD_BUILDER_VERSION prefix)   → deposit.go
  🔵 [x] ResolveIndex (pubkey → builder index)                                 → manager.go
  🔵 [x] Balance query (CanBuilderCoverBid / IsActiveBuilder)                  → balance.go

EL API Extension (STILL NEEDED — ~1 day):
  🟢 [ ] execution/builder.Parameters: add GasLimit *uint64 field
  🟢 [ ] execution/builder/create_block.go: honour per-build GasLimit override
  🟢 [ ] engine_types.PayloadAttributes: add TargetGasLimit + SlotNumber (spec now has these)
  🟢 [ ] Wire PayloadAttributes.TargetGasLimit through ChainReaderWriterEth1.AssembleBlock

Speculative Build + Bid Strategy:
  🟢 [x] BidStrategy interface + FixedMarginStrategy + tests                   → strategy.go, fixed.go
  🟢 [x] SpeculativeBuild (StartBuild, GetResult)                              → speculative.go
  🟢 [x] PreferencesWatcher (WaitForPreferences)                               → preferences.go
  🟣 [x] Preferences notification callback                                     → epbsPool.OnPreferencesReceived
  🟣 [x] HeadWatcher / OnNewHead trigger                                       → loop.go OnNewHead
  🔵 [x] BidSubmitter.SubmitBid                                                → submitter.go

Reveal + BidWon:
  🟢 [x] Pending payload store (keyed by BidMarketKey)                          → loop.go pendingPayloads
  🟢 [x] CaplinBidSubmitter + GossipPublisher                                  → submitter.go
  🔵 [x] BroadcastPayload (gossip + local OnExecutionPayload)                  → submitter.go
  🟣 [x] BidWon emitter (OnBidWonFunc → BlockService)                          → integration.go
  🟢 [x] CLI flags (builder.*, builder.key, builder.fee-recipient)              → cmd/utils/flags.go
  🟢 [x] Config parsing                                                        → epbscfg/config.go
  🟢 [x] Service lifecycle + wiring                                             → integration.go

EL Adapter (not in original plan):
  🟢 [x] PayloadAssembler interface                                             → eladapter/adapter.go
  🟢 [x] AssembledPayload type                                                 → eladapter/types.go

REMAINING — Integration + Devnet:
  🟠 [ ] End-to-end test on local ephemeral devnet (1 proposer + 1 builder)
  🟠 [ ] Measure actual reveal-path latency vs budget table; tune if off
  🟠 [ ] Measure speculative-build hit rate (fast-path vs rebuild-path)
  🟠 [ ] Preferences-mismatch chaos test (inject late/conflicting prefs)
  🟠 [ ] Test on ePBS devnet (epbs-devnet-1 or successor)
  🟠 [ ] Fix remaining timing issues under realistic network conditions
  🟠 [ ] Basic logging (bid submitted, won/lost, reveal timing, profit)
  🟠 [ ] Balance monitoring + low-balance alerts
```

**Remaining work estimate: ~2-3 weeks**
1. 🔴 Deposit bootstrap verification on devnet (blocker, ~1-2 days)
2. 🟢 EL API: `GasLimit` extension + `PayloadAttributes` update (~1 day)
3. 🟢 DataColumnSidecar broadcasting from builder (new GLOAS responsibility, ~1-2 days)
4. 🟠 Integration testing + timing tuning (1-2 weeks)

### Phase 2: Hardening (1-2 weeks)

Priority order reflects actual risk reduction (see "Key-security model" and
"Defense priority" below): balance bounding + circuit breaker before
remote-signer, because for our target user they bound loss more effectively.

**Why not "hot/cold separation" as originally planned.** Partial withdrawal
from a builder's beacon-state balance to a cold wallet does not exist in the
current GLOAS spec (`feature/caplin_gloas`). The only withdrawal primitive is
`InitiateBuilderExit(builderIndex)` (`cl/phase1/core/state/cache_mutators.go:168`)
which is **all-or-nothing** — it sets `WithdrawableEpoch` and the entire
balance eventually leaves via the standard exit sweep. `BuilderPendingWithdrawal`
entries are created only by the **automatic payment flow**
(`process_builder_pending_payments.go`, bid weight transferred from builder
to proposer), not by a user-initiated "sweep profit" RPC. A naive daemon that
tries to call some imagined `partialWithdraw()` will have nothing to bind to
on-chain. Until a partial-withdrawal EIP follow-up lands, the realistic
bounding mechanism is **exit + redeposit cycling**.

```
  Financial safety (highest impact):
  [ ] Bounded hot balance via exit/redeposit cycling
        - --builder.max-hot-balance <wei> — upper cap for balance on a live builderIndex
        - --builder.cold-address <addr> — funding source for fresh deposits
        - Cycle job:
            (0) PRECONDITION: get_pending_balance_to_withdraw_for_builder == 0
                (spec requires all pending payments/withdrawals to settle before exit)
            (1) when balance ≥ max-hot-balance, call InitiateBuilderExit on current builderIndex
            (2) stop bidding on that index; wait for WithdrawableEpoch + sweep to cold address
                (WithdrawableEpoch = current + MIN_BUILDER_WITHDRAWABILITY_DELAY = 8192 epochs ≈ 36 days)
            (3) submit a new deposit (--builder.deposit-amount) from cold address, wait activation
            (4) resume bidding under a NEW builderIndex (old index may be reused by others)
        - Cost: several epochs of non-participation per cycle; acceptable if tuned so cycling
          happens rarely (e.g. every few days at target bid volume). Builder identity changes
          each cycle — fine for in-protocol ePBS (no relay-style reputation routing).
        - Spec-dependency note: if partial-withdrawal primitive ships later, replace
          cycling with a simpler sweep job.
  [ ] Circuit breaker
        - Pause bidding after N consecutive non-reveals (default N=3)
        - Pause on unexpected signer errors, balance delta > threshold, etc.
        - Pause automatically when balance ≥ max-hot-balance while a cycle is
          in-flight (redundant with cycling but catches races)
        - Manual unpause via RPC (avoid auto-recovery mid-incident)
        - Metrics for alerting
  [ ] Safety invariants
        - Never bid if payload not ready (already in Phase 1; re-audit)
        - Never bid if balance < 2× bid value (belt-and-braces on CanCoverBid)
        - Never bid if last N slots had >K non-reveals on this host

  Operational features:
  [ ] Second speculative rebuild with fresher txns at ~1800ms (replace initial spec build if higher value) — MVP does one spec build per slot
  [ ] Explicit Cancel() API on execution/builder.Builder for tidy rebuild (MVP uses concurrent builds + interrupt atomic)
  [ ] Graceful exit flow
  [ ] Top-up transaction support
  [ ] Metrics: bid win rate, reveal latency histogram, P&L tracking

  Keyfile hardening (low-cost Phase 1 extension; listed here for completeness):
  [ ] Keyfile permissions enforcement (refuse to start if mode != 0400)
  [ ] Dedicated-user / core-dump-disabled startup docs
  [ ] GOTRACEBACK=none guard on builder goroutines (avoid key in panic traces)
```

### Phase 3: Optional deployment modes (on demand)

```
  [ ] Web3Signer adapter (cl/builder/epbs/signer_web3.go)
        - POST to /api/v1/eth2/sign/{pubkey} with signing_root
        - Handle BEACON_BUILDER signing type once Web3Signer ships GLOAS support
        - Timeout + retry budget (must respect 3000ms bid deadline — budget ~200ms per sign call)
        - Health check at startup (refuse to start builder if remote signer unreachable)
        - Only meaningfully safer than LocalSigner when the signer runs on a
          different host with smaller attack surface than Erigon. Same-host
          Web3Signer provides negligible benefit.
  [ ] --builder.remote-signer + --builder.pubkey CLI flags
  [ ] Signer swap at runtime (local → remote) without node restart (optional)

  Advanced (future / deferred):
  [ ] Adaptive bid strategy (observe competitor bids from EpbsPool.HighestBids)
  [ ] Multi-block simulation (try different tx orderings, pick most profitable)
  [ ] Bundle API (accept external searcher bundles)
  [ ] Monitoring dashboard (win rate, P&L, bid history)
```

## ePBS Protocol Constraints (from Erigon codebase)

These constraints are derived from the actual validation logic in Caplin.
Builder code must respect all of them or bids will be rejected.

### Bid Validation (cl/phase1/network/services/execution_payload_bid_service.go)

| Check | Result | Implication for builder |
|-------|--------|------------------------|
| `bid.execution_payment != 0` | `[REJECT]` | Must always be 0 (reserved field) |
| `bid.slot != current_slot && bid.slot != current_slot+1` | `[IGNORE]` | Only bid for current or next slot |
| ProposerPreferences not yet received for slot | Queued (pending up to 12s) | Builder should wait for preferences before bidding |
| `bid.fee_recipient != preferences.fee_recipient` | `[REJECT]` | Must use proposer's fee_recipient |
| `bid.gas_limit` not compatible with `preferences.target_gas_limit` | `[IGNORE]` | Spec uses `[IGNORE]`, not `[REJECT]`. Caplin should align to spec. Builder should still match `target_gas_limit` to avoid being ignored. |
| Duplicate bid from same builder+slot | `[IGNORE]` | One bid per (builderIndex, slot); can't update once submitted |
| Builder not active in beacon state | `[REJECT]` | Must deposit and be registered first |
| `builder.version != PAYLOAD_BUILDER_VERSION (uint8(0))` | `[REJECT]` | Builder version must match; fork-onboarded deposits must register with version `0`, not `0x03` |
| Builder can't cover bid value (insufficient balance) | `[IGNORE]` | Balance must exceed bid value |
| Invalid BLS signature (DomainBeaconBuilder) | `[REJECT]` | Must sign with correct domain + GLOAS fork version |
| `bid.parent_block_hash` unknown in fork choice | `[IGNORE]` | Only bid on known chain tips |
| `bid.parent_block_root` unknown in fork choice | `[IGNORE]` | Only bid on known chain tips |
| Bid value lower than existing highest for same market | `[IGNORE]` | Must beat competitors or be ignored |

### State-Transition Validation (additional checks at block processing)

`ProcessExecutionPayloadBid` (`cl/transition/impl/eth2/operations.go:506-586`)
performs all checks at the state-transition level. The gossip-layer checks
above are a subset; these are the full set enforced when the proposer's
block is processed by the network — if any fail, the entire block is invalid.
(Active builder, balance, and signature checks from the gossip table are
re-verified here but omitted below for brevity.)

| Check | Implication for builder |
|-------|------------------------|
| `state.builders[builderIndex].version == PAYLOAD_BUILDER_VERSION` | Builder version must be `uint8(0)`; fork-onboarded deposits must not carry `BUILDER_WITHDRAWAL_PREFIX` into `builder.version` |
| `bid.Slot == block.GetSlot()` | Bid must target the exact slot of the beacon block |
| `bid.ParentBlockHash == state.GetLatestBlockHash()` | Must match state's latest EL block hash (GLOAS: not from header) |
| `bid.ParentBlockRoot == block.GetParentRoot()` | Must match the beacon block's parent root |
| `bid.PrevRandao == state.GetRandaoMixes(epoch)` | Randao must match current epoch's mix in state |
| `bid.BlobKzgCommitments.Len() <= get_blob_parameters(epoch).max_blobs_per_block` | Blob limit is epoch-dependent (not a fixed `MaxBlobsPerBlock`) |
| Self-build: `Signature == bls.G2_POINT_AT_INFINITY && Value == 0` | Only for `BuilderIndexSelfBuild = MaxUint64`. Self-build envelope is signed by the **proposer's** validator key, not a builder key. |

The builder must ensure `PrevRandao` matches the beacon state's randao mix
at the target epoch. This value is available from the fork choice state
after the parent block's randao reveal is processed.

### Bid Market Key

```go
// cl/pool/epbs_pool.go — HighestBids cache
type HighestBidKey struct {
    Slot            uint64
    ParentBlockHash common.Hash   // EL parent
    ParentBlockRoot common.Hash   // CL parent
}
```

Same slot can have multiple independent bid markets (e.g., after reorg).
The `is_highest` flag is per-market, not per-slot.

### PTC Voting (Payload Timeliness Committee)

- **PtcSize**: 512 validators per slot
- **Voting window**: 75%-100% of slot (9000-12000ms)
- PTC members vote `PayloadAttestationMessage` with:
  - `payload_present`: did they see the execution payload envelope?
  - `blob_data_available`: is blob data available?
- **Two independent thresholds** (simple majority, NOT supermajority):
  - `payload_present` count > `PayloadTimelyThreshold` (256 = PtcSize/2)
  - `blob_data_available` count > `DataAvailabilityTimelyThreshold` (256 = PtcSize/2)
  - See `cl/clparams/constants.go:12-14`, `cl/phase1/forkchoice/payload_vote.go:147,176`
- **BOTH** thresholds must pass for `ShouldExtendPayload` → true → PayloadStatus=Full
  (`payload_vote.go:266`: `isPayloadTimely(root) && isPayloadDataAvailable(root)`)
- If either fails → PayloadStatus=Empty (builder loses bid value)
- **Implication for builder**: must ensure BOTH the envelope AND blob data are
  delivered to PTC members before 9000ms. Missing blobs alone is enough to fail.

### Envelope Verification (complete checklist)

`verify_execution_payload_envelope` checks all of the following — if any
fail, the envelope is rejected by the network:

| Check | What the builder must ensure |
|-------|------------------------------|
| `verify_execution_payload_envelope_signature(state, signed_envelope)` | Sign with correct builder key + `DomainBeaconBuilder` |
| `envelope.beacon_block_root == hash_tree_root(header)` | Set to the root of the beacon block that committed the winning bid |
| `envelope.parent_beacon_block_root == state.latest_block_header.parent_root` | Set to the parent beacon block root |
| `envelope.builder_index == bid.builder_index` | Must match the bid |
| `payload.prev_randao == bid.prev_randao` | Must match the bid |
| `payload.gas_limit == bid.gas_limit` | Must match the bid |
| `payload.block_hash == bid.block_hash` | Must match the bid |
| `hash_tree_root(envelope.execution_requests) == bid.execution_requests_root` | Requests root must match |
| `payload.slot_number == state.slot` | Must be the current slot |
| `payload.parent_hash == state.latest_block_hash` | Must match state's latest EL block hash |
| `payload.timestamp == compute_time_at_slot(state, state.slot)` | Must be the slot's timestamp |
| `hash_tree_root(payload.withdrawals) == hash_tree_root(state.payload_expected_withdrawals)` | Withdrawals must match expected set exactly |
| `execution_engine.verify_and_notify_new_payload(...)` | EL must accept the payload |

### DataColumnSidecar: builder's responsibility

In GLOAS, proposers **no longer** broadcast `DataColumnSidecar` objects.
This duty transfers to the builder. After constructing the envelope, the
builder must also compute and broadcast data column sidecars for any blob
transactions in the payload. Failure to broadcast blob data means PTC
members vote `blob_data_available=false` → payload marked EMPTY → bid
value lost.

### Regular attestation payload status signaling

In addition to PTC votes, regular attestations now signal payload status
via `attestation.data.index`:
- `data.index = 0` → attester considers payload status EMPTY
- `data.index = 1` → attester considers payload status FULL (requires
  `is_payload_verified(store, beacon_block_root)`)

This means fork choice weight depends on both PTC votes AND regular
attestations' index values. The builder should care because this affects
`should_extend_payload` and `should_build_on_full` outcomes for the
NEXT slot's parent determination.

### Proposer slashing clears builder payment

If a proposer is slashed (`process_proposer_slashing`), the builder's
pending payment for that proposer's slot is zeroed out:
```python
state.builder_pending_payments[payment_index] = BuilderPendingPayment()
```
The builder loses revenue through no fault of its own. This is a rare
edge case but should be accounted for in P&L tracking.

### Builder exit constraints

Two exit paths exist:
1. **CL-side**: BLS-signed `VoluntaryExit` with builder index (bit 40 set).
   Processed by `ProcessVoluntaryExit` which detects `IsBuilderIndex` and calls
   `InitiateBuilderExit`.
2. **EL-side (EIP-8282)**: `BuilderExitRequest` (`BUILDER_EXIT_REQUEST_TYPE = 0x04`)
   via the builder exit contract. Authorized by `execution_address` (not BLS key).
   The request exits the builder; it does not carry a partial-withdrawal amount.
   Processed by `process_builder_exit_request`.

Common preconditions for both paths:
- Builder must be active (`is_active_builder`)
- `get_pending_balance_to_withdraw_for_builder(state, builder_index) == 0`

All pending payments and withdrawals must settle before exit is allowed.
Phase 2's exit/redeposit cycling must wait for settlement.

### Builder index reuse

`get_index_for_new_builder` reuses indices of exited builders with zero
balance. Phase 2 cycling gets a **different `builderIndex`** each time
because the old index is freed and may be reassigned to another deposit.

### Builder Registration

```go
// cl/clparams/config.go
BuilderRegistryLimit               = 1 << 40   // max ~1T builders (cl/clparams/config.go)
MinBuilderWithdrawabilityDelay     = 8192       // 2^13 epochs (~36 days) before withdrawal (cl/clparams/config.go)
BuilderPaymentThresholdNumerator   = 6          // (cl/clparams/constants.go)
BuilderPaymentThresholdDenominator = 10         // 60% threshold for payment (cl/clparams/constants.go)
```

## Key Advantage: Same-Process Communication

```
Other clients (Geth + Prysm/Lighthouse):
  Geth ←─ Engine API (HTTP) ─→ CL ←─ Beacon API (HTTP) ─→ bid submission
  ~2-5ms per round-trip, at least 2 round-trips

Erigon + Caplin:
  Block Builder ──→ BuilderLoop ──→ Caplin bid service
  Direct function call, ~microseconds
```

In ePBS timing games, milliseconds matter. This latency advantage is structural and cannot be replicated by other client combinations without architectural changes.

## Builder Deposit Mechanism

Builder deposit is a CL operation analogous to validator deposit, but with a distinct
withdrawal credentials prefix and registry. Deposits are processed by
`ApplyDepositForBuilder` (`cl/phase1/core/state/epbs.go` on `feature/caplin_gloas`).

### Deposit flow

1. **Generate BLS key pair** — builder's identity key (independent from any validator key)
2. **Construct `DepositData`**:
   - `pubkey`: builder BLS public key
   - `withdrawal_credentials`: prefix `PAYLOAD_BUILDER_VERSION` (`0x00`) + 11 zero bytes + 20-byte execution address (where payments withdraw to). Note: `BUILDER_WITHDRAWAL_PREFIX` (`0x03`) is only used for fork-onboarding of legacy pending deposits.
   - `amount`: at least `MinDepositAmount` (1 ETH = 1e9 Gwei)
   - `signature`: BLS signature over deposit message (`DomainBuilderDeposit` `0x0E000000` with genesis fork version) — proof of possession
3. **Submit via `BuilderDepositRequest`** — EIP-8282 builder deposit contract (separate from the validator deposit contract). The spec defines `BUILDER_DEPOSIT_REQUEST_TYPE = 0x03` as a distinct execution request type.
4. **Caplin processes deposit**:
   - New pubkey → verify signature → `AddBuilderToRegistry` assigns next free index
   - Existing pubkey → `builder.Balance += amount` (top-up)
5. **Wait for activation** — `DepositEpoch < finalized_checkpoint.epoch` before `IsActiveBuilder` returns true (typically 2–3 epochs)

### Balance semantics

```go
// cl/cltypes/epbs_builder.go
type Builder struct {
    Pubkey            common.Bytes48
    Version           uint8           // must be PAYLOAD_BUILDER_VERSION (uint8(0)) to bid
    ExecutionAddress  common.Address  // from withdrawal_credentials[12:]
    Balance           uint64          // debited when bid is committed
    DepositEpoch      uint64
    WithdrawableEpoch uint64          // FarFutureEpoch until exit initiated
}

// Available to bid = Balance - MinDepositAmount - pending withdrawals
func CanBuilderCoverBid(s, builderIndex, bidAmount) bool
```

Key point: **once bid is committed in a beacon block, `bid.Value` is deducted
regardless of whether the payload is revealed**. Non-reveal is a direct loss,
not a slashing event.

### `BuilderManager.Deposit()` responsibilities

- Construct `DepositData` with `PAYLOAD_BUILDER_VERSION` (`0x00`) prefix + execution address
- Sign with `DomainBuilderDeposit` (`0x0E000000`)
- Submit via EIP-8282 builder deposit contract (`BuilderDepositRequest`)
- Poll beacon state until `ResolveIndex` succeeds (pubkey appears in registry)
- Track `DepositEpoch`; only allow bidding once finalized

### Devnet bootstrapping (Phase 1 W1 🔴 blocker)

Every part of the build-bid-reveal loop depends on a live `builderIndex` in the
beacon state. `CanBuilderCoverBid` returns false for pubkeys that haven't
deposited + activated, so a builder without a working bootstrap path emits
bids that are all `[REJECT]`ed during validation — no integration test works.

**The compatibility point is the CL execution-request path.** New builder
deposits are EIP-8282 `BuilderDepositRequest`s, distinct from validator
`DepositRequest`s. They carry `PAYLOAD_BUILDER_VERSION` (`uint8(0)`) withdrawal
credentials and signatures under `DOMAIN_BUILDER_DEPOSIT` (`0x0E000000`).
Caplin must route those requests to `ApplyDepositForBuilder` (not validator
deposit processing). `BUILDER_WITHDRAWAL_PREFIX` (`0x03`) is temporary and only
applies to fork-onboarding legacy pending deposits.

**W1 deliverables** (see Phase 1 Week 1 checklist):
1. **CL-side routing verification**: on `feature/caplin_gloas`, trace the
   execution request path and confirm `BuilderDepositRequest` dispatches to
   `ApplyDepositForBuilder` rather than silently falling through to validator
   processing. Add a unit test if the branch is not already covered.
2. **Fork activation**: confirm GLOAS is active at target devnet genesis
   (or scheduled to activate before the first test slot). Outside GLOAS,
   builder deposit requests are invalid and will be dropped.
3. **Funding**: identify source (devnet faucet, genesis allocation, or
   funded key file) for the cold address.
4. **Tooling**: ship `erigon builder deposit --amount <N>ETH --key <bls>`
   subcommand. Builds `BuilderDepositRequest{pubkey,
   WithdrawalCredentials=PAYLOAD_BUILDER_VERSION||zero[11]||cold_addr, amount,
   signature=SignDeposit(root, DOMAIN_BUILDER_DEPOSIT(genesis_fork))}` and
   submits it through the EIP-8282 builder deposit contract.
5. **End-to-end smoke**: deposit request included → Caplin decodes
   `BuilderDepositRequest` → state transition runs `ApplyDepositForBuilder` →
   `IsActiveBuilder(index)` returns true after activation.
6. **Activation-epoch measurement**: record the actual delay observed on
   the target devnet so W4 integration tests can schedule `bootstrap +
   wait` correctly (don't assume a fixed value).

If step 1 or 2 fails, the blocker is **`feature/caplin_gloas` CL-team
coordination** (and possibly spec work), not devnet-operator coordination.
Surface that status publicly (PR description + any status tracker) rather
than quietly writing code nobody can exercise.

## Development Dependency on caplin_gloas

The ePBS builder depends on types and services that currently live on
`feature/caplin_gloas`, not `main`. After cross-referencing the branch, remaining
work splits cleanly into three partitions.

### Partition A — caplin_gloas 已提供 (reuse as-is)

Found on `feature/caplin_gloas`, no new work needed beyond calling it:

| Capability | Location | Status |
|-----------|----------|--------|
| EL → CL block conversion | `ExecutionClientDirect.GetAssembledBlock` returns `*cltypes.Eth1Block` directly | ✅ Reuse |
| Build + bid reference pattern | `cl/beacon/handler/block_production.go:615-1051` (`produceBeaconBody`: FULL/EMPTY branching → FCU → polling → payload assembly → requests bundle → bid selection) | ✅ Template |
| Blob bundle handling | Same file; `BlobsBundle` caching + `BlobKzgCommitments` extraction. **GLOAS note**: `BlobKzgCommitments` are NOT added to `beaconBody.BlobKzgCommitments` for GLOAS — they live in the bid instead (see line 740-744). | ✅ Template (adapt for GLOAS) |
| Envelope disk persistence | `HasEnvelope` / `ReadEnvelopeFromDisk` / `StoreAnchorEnvelope` on `ForkChoiceStorage` | ✅ Reuse |
| Envelope validation pipeline | `OnExecutionPayload(ctx, signedEnvelope, checkBlobData, validatePayload)` | ✅ Reuse |
| Bid validation (inbound) | `execution_payload_bid_service.go` — sig, preferences, balance, market key, pending queue (100ms check, 12s expiry) | ✅ Reuse |
| Preferences validation | `proposer_preferences_service.go` — BLS `DomainProposerPreferences`, writes to `epbsPool.ProposerPreferences` | ✅ Reuse |
| All ePBS types | `cl/cltypes/epbs_*.go` — `ExecutionPayloadBid`, `ExecutionPayloadEnvelope`, `ProposerPreferences`, etc. | ✅ Reuse |
| State accessors | `cl/phase1/core/state/epbs.go` — `ApplyDepositForBuilder`, `IsActiveBuilder`, `CanBuilderCoverBid`, `IsBuilderIndex`, `AddBuilderToRegistry` | ✅ Reuse |
| BeaconBlock body carries bid | `SignedBeaconBlock.Body.SignedExecutionPayloadBid` (GLOAS) — bid inclusion is how "winning" is signalled on-chain | ✅ Reuse |
| Latest EL block hash | `BeaconState.GetLatestBlockHash()` — replaces `LatestExecutionPayloadHeader().BlockHash` in GLOAS (header no longer updated) | ✅ Reuse |
| FULL/EMPTY determination | `ForkChoiceStorageReader.ShouldExtendPayload(root)` — PTC-based (timely + data available). **Note**: builder should use `ShouldBuildOnFull(head)` (different function) to decide EL parent hash. | ✅ Reuse |
| Build-on-full determination | `ShouldBuildOnFull(store, head)` — returns true if head is FULL and blob data not voted unavailable. This is what the builder uses for parent hash selection. | ✅ Reuse (verify exists) |
| Payload status query | `ForkChoiceStorageReader.GetHeadPayloadStatus()` — returns FULL, EMPTY, or PENDING for current head. **Note**: `get_head()` now returns `ForkChoiceNode(root, payload_status)`, not just a root. | ✅ Reuse |
| Blob data availability | `ForkChoiceStorageReader.IsBlobDataAvailable(slot, root)` — check blob data status for a block | ✅ Reuse |
| Deferred payload processing | `ProcessParentExecutionPayload` (`operations.go:588`) — execution effects applied at start of NEXT block, not during envelope processing | ✅ Reuse (understand flow) |
| SSE events | `execution_payload_bid` (bid validated), `payload_attestation_message` (PTC vote), `execution_payload_available` (envelope processed) — available via Beacon API `/events` | ✅ Reuse (optional signals) |
| Engine API V5/V6 | GLOAS uses `NewPayloadV5`/`GetPayloadV6`; `Eth1Block` now includes `BlockAccessList` and `SlotNumber` | ✅ Reuse |
| Bid selection in proposer | `block_production.go:465-497` — proposer checks `epbsPool.HighestBids` for external bids that beat self-build value | ✅ Reuse (confirms builder flow) |

**Impact**: the biggest previously-identified gap ("EL→CL conversion") is fully
solved by the existing engine client. `block_production.go` supplies a working
template for the entire build path including requests bundle and blob handling.
With deferred payload processing, the builder's envelope effects are not applied
until the next block — this simplifies state reasoning but means the builder
must understand the FULL/EMPTY parent branching for correct EL parent hash
determination.

**Fork choice API changes in GLOAS**:
- `get_head()` returns `ForkChoiceNode(root, payload_status)` instead of `Root`.
  Every node in the fork choice tree carries a `PayloadStatus` (EMPTY/FULL/PENDING).
- `LatestMessage` changed from epoch-based to slot-based, with a `payload_present`
  boolean: `LatestMessage{slot, root, payload_present}`. This affects fork choice
  weight calculation via `is_supporting_vote`.
- Regular attestations signal payload status via `data.index` (0=EMPTY, 1=FULL),
  not just PTC votes.

### Partition B — caplin_gloas 需擴充 ✅ ALL DONE

All four Partition B items have been implemented on `feature/epbs_builder`.
No further CL-side coordination is needed for these.

| Addition | Status | Implementation |
|----------|--------|---------------|
| `ShouldExtendPayload` export | ✅ DONE | `cl/phase1/forkchoice/payload_vote.go:259` — already exported with capital S |
| Preferences notification on arrival | ✅ DONE | `cl/pool/epbs_pool.go` has `OnPreferencesReceived` callback → `cl/builder/epbs/preferences.go` `PreferencesWatcher.WaitForPreferences(slot, timeout)` consumes it |
| **BidWon event emitter** | ✅ DONE | `cl/builder/epbs/loop.go:306` `OnBidWon(ctx, slot, parentHash, parentBlockRoot, beaconBlockRoot)` + `cl/builder/epbs/integration.go:261` `OnBidWonFunc()` wires it to `BlockService.OnBidWon` |
| Gossip publisher for bid/envelope | ✅ DONE | `cl/builder/epbs/submitter.go:78` `CaplinBidSubmitter.BroadcastPayload()` publishes via `GossipManager.Publish` on `TopicNameExecutionPayload` |
| ~~`ActiveParents(slot)`~~ | N/A | Functionality absorbed into `BuilderLoop.OnNewHead` / `OnSlot` with `SlotContext` parameter |

### Partition C — 本套件新寫 ✅ MOSTLY DONE

All core builder files exist on `feature/epbs_builder` under `cl/builder/epbs/`.
See "Files to Create — Implementation Status" section for detailed inventory.

**Actual implementation** (21 files) differs from original plan in naming:
- `caplin.go` → became `integration.go` (service init + lifecycle + OnBidWonFunc)
- `head.go` / `markets.go` → absorbed into `loop.go` (OnNewHead with SlotContext)
- `value.go` → not created (reuses `execmodule.blockValue` as planned)
- `builder_flags.go` → flags in `cmd/utils/flags.go` (not a separate file)
- **New additions not in plan**: `eladapter/` package, `epbscfg/` package,
  `integration_test.go`, `deposit_test.go`

### Work-ordering status

1. ~~Week 1 on `main`~~: **DONE** — Partition C interfaces + logic implemented
2. ~~Week 1–2 Partition B PRs~~: **DONE** — all 4 CL hooks implemented
3. ~~Week 2–3 wire to real surfaces~~: **DONE** — `integration.go` wires everything
4. **Remaining**: EL `GasLimit` gap + devnet integration testing

## Resolved Questions

- **Slot timing**: Caplin has a slot ticker; BuilderLoop hooks into it. BPS-based
  deadlines: bid before 2500 BPS (3000ms), reveal before 7500 BPS (9000ms).
- **Bid won notification**: Watch for our `builderIndex` in the committed beacon block's
  bid field. Caplin's `on_block` processing can trigger `OnBidWon` callback (~10-line
  hook, see "Impact surface on caplin_gloas" above).
- **Builder deposit**: CL operation — see "Builder Deposit Mechanism" above. Uses
  `PAYLOAD_BUILDER_VERSION` (`uint8(0)`) in withdrawal credentials and
  `DOMAIN_BUILDER_DEPOSIT` (`0x0E000000`). `BUILDER_WITHDRAWAL_PREFIX` (`0x03`)
  is temporary fork-onboarding-only.
- **Package location**: `cl/builder/epbs/`, not `execution/builder/`. Maintains the
  existing `cl → execution` dependency direction. See "Package Location" section.
- **Can development parallelize with caplin_gloas?**: Yes. ~40–50% of work has no
  CL dependency; the rest can be built directly on the `feature/caplin_gloas` branch
  since ePBS types/services are already stable there.
- **Rebuild closer to deadline?**: Speculative build is **MVP**, not Phase 2 — the
  3000ms bid deadline makes sequential "wait-then-build" infeasible. Start EL build
  on OnNewHead with provisional attributes; rebuild if preferences differ. Phase 2
  may add *second* speculative build with fresher txns if time permits.
- **Non-reveal penalty mechanism**: When a bid is committed in a beacon block,
  `ProcessExecutionPayloadBid` (`operations.go:566-579`) creates a
  `BuilderPendingPayment{Weight: 0}`. Attestation processing accumulates
  `Weight` from attesting validators' effective balances. At epoch boundary,
  `ProcessBuilderPendingPayments` promotes the payment to a builder withdrawal
  only if `Weight >= quorum` (60% of per-slot balance). On successful reveal,
  `ApplyParentExecutionPayload` queues the payment to the proposer's
  `fee_recipient` and deducts from builder balance. On non-reveal, the PTC
  votes empty → payment Weight may not reach quorum → payment discarded, but
  `CanBuilderCoverBid` still reserves the balance for pending payments,
  effectively locking the builder's funds. Not slashing, but a financial loss
  through locked/deducted balance. BuilderLoop must NEVER bid without a ready
  payload.

## Remaining Open Questions

- [x] How to detect fork choice updates mid-slot that create new bid markets?
      → **Resolved**: `BuilderLoop.OnNewHead(ctx, SlotContext)` is triggered by
      fork-choice head changes. `SlotContext` carries the parent info.
- [ ] Should builder maintain a minimum balance reserve (e.g., 2x max bid) and auto-pause if below?
- [x] Blob handling: `BlobKzgCommitments` extracted from EL block's blob txns
      and included in bid. `ExecutionRequestsRoot = hash_tree_root(execution_requests)`
      also required. Both verified by state transition and envelope validation.
- [ ] Does a partial-withdrawal primitive for builders land in a GLOAS follow-up
      EIP? If so, Phase 2 "exit/redeposit cycling" can be replaced with a simple
      sweep job. Track via spec discussion channels.
- [x] How does the deposit transaction work exactly on devnet?
      → **Promoted to Phase 1 🔴 blocker** (verify on target devnet;
      ensure BuilderDepositRequest is available and routed to `ApplyDepositForBuilder`).
- [ ] `Parameters.GasLimit` gap: EL `execution/builder/parameters.go` still lacks
      `GasLimit *uint64`. The `eladapter.Adapter` may work around this, but
      per-build gas limit override is needed for bid validation compliance.
      **Update**: spec now has `target_gas_limit` in `PayloadAttributes`, so
      `engine_types.PayloadAttributes` should also be extended.
- [ ] DataColumnSidecar broadcasting: builder must broadcast blob data column
      sidecars (new GLOAS responsibility, transferred from proposer). Need to
      identify existing column sidecar construction code and wire it into
      the builder's reveal path.
- [ ] Withdrawals computation for FULL parent: builder needs to copy state,
      apply `apply_parent_execution_payload`, then call `get_expected_withdrawals`
      to get the correct withdrawal set. Verify this is implemented correctly
      in the EL adapter / loop.
- [ ] `ExecutionRequests` struct needs `builder_deposits` (`List[BuilderDepositRequest]`)
      and `builder_exits` (`List[BuilderExitRequest]`) fields per spec. Currently
      Erigon routes builder deposits/exits through existing `DepositRequest` /
      `WithdrawalRequest` processing with prefix checks. CL team dependency.
- [ ] Honest payload withholding: implement the strategy described above in
      `OnBidWon` to avoid revealing payloads for blocks that are not timely and
      not canonical.

## Files to Create — Implementation Status

Last verified: 2026-04-22 against `feature/epbs_builder` branch.

```
✅ = exists on branch    ⬜ = not yet done    🔧 = still needed

cl/builder/epbs/ (core builder package):
  ✅ loop.go                   -- slot-driven build+bid+reveal loop (OnNewHead, OnSlot, OnBidWon)
  ✅ loop_test.go
  ✅ speculative.go            -- SpeculativeBuild: StartBuild, GetResult
  ✅ preferences.go            -- PreferencesWatcher: WaitForPreferences + OnPreferencesReceived
  ✅ signer.go                 -- Signer interface (Pubkey, SignBid, SignEnvelope, SignDeposit)
  ✅ signer_local.go           -- LocalSigner: in-process BLS from keyfile
  ✅ signer_local_test.go
  ⬜ signer_web3.go            -- Web3Signer adapter (Phase 3, not started)
  ✅ manager.go                -- BuilderManager: signing, ResolveIndex, identity
  ✅ manager_test.go
  ✅ deposit.go                -- deposit + index resolution
  ✅ deposit_test.go           -- (not in original plan)
  ✅ balance.go                -- beacon state balance monitoring
  ✅ strategy.go               -- BidStrategy interface
  ✅ strategy_test.go
  ✅ fixed.go                  -- FixedMarginStrategy
  ✅ submitter.go              -- BidSubmitter + CaplinBidSubmitter + GossipPublisher interfaces
  ✅ integration.go            -- BuilderService init, lifecycle, OnBidWonFunc (was planned as caplin.go)
  ✅ integration_test.go       -- (not in original plan)

cl/builder/epbs/ (sub-packages, not in original plan):
  ✅ eladapter/adapter.go      -- EL adapter: PayloadAssembler interface
  ✅ eladapter/types.go        -- AssembledPayload type
  ✅ epbscfg/config.go         -- Config struct (Enabled, KeyPath, FeeRecipient, BidMargin, MinProfit)

Planned but absorbed into existing files:
  ❌ head.go                   -- HeadWatcher functionality absorbed into loop.go OnNewHead
  ❌ markets.go                -- multi-market logic absorbed into loop.go (SlotContext-based)
  ❌ caplin.go                 -- renamed to integration.go

CLI flags (not a separate file):
  ✅ cmd/utils/flags.go        -- builder.*, builder.key, builder.fee-recipient, etc.
  ✅ cmd/caplin/caplin1/run.go -- epbs.InitBuilderService wiring + OnBidWonFunc

EL-side changes:
  🔧 execution/builder/parameters.go          -- STILL NEEDED: add GasLimit *uint64 field
  🔧 execution/builder/create_block.go:88     -- STILL NEEDED: honour per-build GasLimit override
  🔧 execution/engineapi/engine_types/jsonrpc.go -- STILL NEEDED: add TargetGasLimit + SlotNumber to PayloadAttributes
  # (no new value.go — reuse execmodule.blockValue from
  #  execution/execmodule/block_building.go:84)

New GLOAS responsibilities (not in original plan):
  🔧 DataColumnSidecar broadcasting           -- Builder must broadcast blob data column sidecars
                                                  (proposers no longer do this in GLOAS)
```

## Risks and Honest Assessment

### Operational risks (new — from protocol analysis):
- **Reveal latency is the kill zone**: If envelope arrives >9000ms into slot, PTC votes
  empty and builder loses bid value. Network jitter, slow EL state root computation, or
  gossip delays can push past deadline. Must budget ~2500ms for propagation.
- **Remote-signer latency budget**: A Web3Signer round-trip adds ~50–200ms to every
  bid and envelope signature. Within the 3000ms bid deadline this is fine; within the
  ~6500ms reveal target it's also fine — but a flaky signer network path can push
  either signature past its deadline. Signer health checks at startup and a retry
  budget < 200ms per call are mandatory before flipping remote-signer on.
- **Preferences dependency**: If proposer's `SignedProposerPreferences` arrives late or
  not at all, builder can't bid for that slot. No preferences = no revenue for that slot.
- **Balance drain**: Consecutive non-reveals (bugs, network issues) drain builder balance
  quickly. Need circuit breaker: pause bidding after N consecutive failures.
- **One-bid-per-builder dedup**: Caplin ignores duplicate bids from same
  (builderIndex, slot). Can't update a bid once submitted. Different builders
  compete via highest-value-wins per market key — a higher bid from another
  builder replaces a lower one. Must get amount right the first time.
- **Blob handling complexity**: Bids include `BlobKzgCommitments` count. Builder must
  correctly extract blob commitments from EL transactions. Mismatch → invalid payload.
  Additionally, **builder is responsible for broadcasting DataColumnSidecars** in GLOAS
  (proposers no longer do this). Missing blob data → PTC votes `blob_data_available=false`
  → payload EMPTY → bid value lost.
- **Proposer slashing risk**: If the proposer who committed our bid is slashed
  (`process_proposer_slashing`), the corresponding `BuilderPendingPayment` is zeroed out.
  The builder loses revenue through no fault of its own. Rare but unrecoverable.
- **Exit timing risk**: `MIN_BUILDER_WITHDRAWABILITY_DELAY = 8192 epochs ≈ 36 days`.
  Phase 2 exit/redeposit cycling has a ~36 day cooldown per cycle, during which
  the builder cannot bid with the old index and hasn't activated the new one yet.

### Key-security model:

**Threat model**:
- Builder keys are **non-slashable**. A leaked key lets an attacker submit bids the
  attacker won't reveal, draining the deposit balance via non-reveal penalty.
  Max loss = current balance; no protocol-level penalty, no correlated slashing.
- Go's memory safety eliminates RCE-via-memory-corruption from Erigon's attack
  surface; the realistic threats are supply-chain attacks on deps, local shell
  compromise, or disk/backup theft.
- **Local shell compromise is game over regardless of signer topology** — an
  attacker with shell on the Erigon host can read keyfiles, `/proc/{pid}/mem`, or
  proxy through a local Web3Signer socket. Remote signing only helps when the
  signer lives on a *different* host.

**Precedent — embedded keys in production CLs**:

| Client | BN + VC mode | Default? | Notes |
|--------|-------------|----------|-------|
| Nimbus | embedded (single process) | ✅ default | Nim; ships Nimbus Unified Client that even embeds EL in the same process |
| Teku | embedded or separate | ✅ recommended for solo stakers | ConsenSys officially recommends single-process when BN+VC on same host |
| Prysm | separate binaries, RPC-connected | ❌ separate only | No embedded mode |
| Lighthouse | separate binaries | ❌ separate only | sigp has declined feature requests for embedded mode |
| Lodestar | separate binaries (same host) | ❌ separate only | — |

Nimbus and Teku have run embedded VC+BN in mainnet production for years with
**no key-in-process incidents**. Prysm/Lighthouse separate their VC primarily
for operational reasons (slashing protection DB isolation, multi-BN failover,
graceful restarts, client diversity) — none of which apply to a builder:
builders have no slashing protection DB, no HA need (1 key, 1 deposit), and
are Erigon-specific by design. The embedded-key precedent therefore transfers
cleanly from Nimbus/Teku's VC to our ePBS builder.

**Defense priority (by actual risk reduction, not theatre)**:

1. **Hot/cold balance separation** (Phase 2): keep only `max_bid × N` on the
   hot builder; sweep profit to a cold address on a schedule. Caps loss from key
   leak to the hot balance — this is the single highest-value mitigation.
2. **Circuit breaker** (Phase 2): auto-pause bidding after N consecutive
   non-reveals or unexpected behaviours. Bounds damage from bugs *and* from
   compromise in progress.
3. **Keyfile hardening** (Phase 1): 0400 permissions, dedicated erigon OS user,
   disabled core dumps, `GOTRACEBACK=none` in builder paths.
4. **Disk-at-rest encryption**: FDE on the Erigon host; protects against
   physical theft and backup leaks. Orthogonal to signer topology.
5. **Remote signer (Web3Signer)** (Phase 3, optional): key on a different host.
   Meaningful only when the signer host has a smaller attack surface than
   Erigon — e.g. no internet exposure, dedicated to signing. On the same host
   it's largely theatre.
6. **HSM**: BLS12-381 HSM support is not broadly available; not recommended.

**Honest conclusion**: for Erigon's target user (node operator running
`--builder` with a modest deposit), embedded keys are **acceptably safe** given
the Nimbus/Teku precedent and the non-slashable threat model. Hot/cold
separation + circuit breaker buy more real safety than process separation.

### Market concerns:
- **Market size**: Currently ~5-10 professional builders, all using custom tools
- **ePBS may not change builder concentration**: Same order flow advantages persist
- **Revenue for casual builders may be very low**: Without MEV strategies, just gas fees

### Why it's still worth doing:
- **Low cost**: 3-4 weeks on top of existing code, not a separate product
- **Erigon differentiator**: No other client has this, good for Erigon adoption
- **Learning**: Deep understanding of ePBS builder mechanics
- **Foundation**: If builder market grows post-ePBS, we're ready
- **Dogfooding**: Using our own ePBS implementation from the builder side finds bugs
- **Censorship resistance**: Local builder provides fallback when external builders censor
