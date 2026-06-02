# Changelog

## [3.5.0] – TBD

### Breaking Changes

#### `--prune.mode=full`: EIP-8252 retention window replaces pre-merge history-expiry

Full mode now retains state and block data for the last `262,144` blocks (~36.4 days), matching [EIP-8252](https://github.com/ethereum/EIPs/pull/11601)'s `REORG_RETENTION_WINDOW` — the inactivity-leak-bounded non-finality window across which an EL must be able to reconstruct state to handle any reorg without external sync. Previously full mode pruned only pre-merge block data ([EIP-4444](https://eips.ethereum.org/EIPS/eip-4444) history-expiry) and kept the last 100,000 blocks of state history.

**What changed:**

| | Before | After |
|---|---|---|
| State history retention | last 100,000 blocks | last 262,144 blocks |
| Block data retention | pre-merge pruned, all post-merge kept (EIP-4444) | last 262,144 blocks |

`--prune.mode=blocks` keeps the same shape (all block data retained) but its `History` retention also bumps from 100,000 to 262,144 blocks. `--prune.mode=minimal` is unchanged — both `Blocks` and `History` retain the 100,000-block window, deliberately sub-EIP-8252 for disk-constrained operators. See [#21342](https://github.com/erigontech/erigon/pull/21342) for details.

**Migration:** existing datadirs upgrade automatically. The prune-config guard now accepts finite distance changes on `History`/`Blocks` in either direction, plus either-direction transitions between a finite Distance and the `KeepPostMergeBlocksPruneMode` chain-history-expiry sentinel on `Blocks` (so the upgrade is silent, and operators can revert with `--prune.distance.blocks=18446744073709551615` even after the auto-upgrade has rewritten the persisted value). Operators who want to keep the old "retain all post-merge block data" behavior can switch to `--prune.mode=blocks` or pass the override flag.

Note: physical deletion of frozen `.seg` files is gated by [#21306](https://github.com/erigontech/erigon/issues/21306); existing on-disk segments persist until that lands. The config-level transition is still recorded so the new cutoff takes effect once the deletion path exists.

---

#### Single p2p listener: `--p2p.allowed-ports` removed, all eth versions multiplex on `--port`

Erigon now opens a single TCP listener on `--port` (default 30303) carrying every configured eth protocol version, instead of one listener per protocol on 30303/30304/30305. This fixes a discovery-DHT race that left inbound peers stuck at a fraction of `--maxpeers` for multi-protocol deployments — per-protocol Servers each signed an ENR under the same Node ID, and only the highest-`seq` one survived in the DHT, so peers dialed the wrong listener (#21335).

**What changed:**

| Aspect | Before | After |
|---|---|---|
| Inbound peer ports | `30303`, `30304`, `30305`, … (one per eth version) | `30303` only |
| `--p2p.allowed-ports` flag | Picked one port per protocol from this list | **Removed** — passing it now errors |
| `--maxpeers` semantics | Per-protocol cap; actual ceiling ≈ N × maxpeers | Honest total cap |
| Default `--maxpeers` | `32` | `64` (compensates for the now-honest cap) |
| Enode database directory | `<datadir>/nodes/eth68`, `<datadir>/nodes/eth69`, … | `<datadir>/nodes/eth` |

**Migration:**

- Remove `--p2p.allowed-ports=...` from CLI args / config files; it is no longer recognised.
- Firewall, Kubernetes Service, and monitoring rules that explicitly opened 30304/30305 can drop those entries — only `--port` is bound now.
- If you previously lowered `--maxpeers` because you knew the per-protocol multiplication inflated the real ceiling, raise it back to the target total (the cap is now what the flag says).
- First run after upgrade loses the warm peer cache in `nodes/eth{68,69,…}` — nothing on disk is deleted, the directories are simply no longer read; discovery rebuilds the peer set from bootnodes within a few minutes.

Standalone `sentry` binary (`cmd/sentry`) and `--sentry.api.addr` (remote sentry over gRPC) are unaffected — neither had the bug.

---

#### `debug_trace*` RPC: `enableMemory` / `enableReturnData` replace `disableMemory` / `disableReturnData`

Aligns Erigon with the execution-apis specification ([ethereum/execution-apis#762](https://github.com/ethereum/execution-apis/pull/762)) and Geth behavior.

**What changed:**

| Field | Before (Erigon) | After (Erigon / Geth / Spec) |
|-------|-----------------|------------------------------|
| Memory in trace | `disableMemory` (default: included) | `enableMemory` (default: excluded) |
| Return data in trace | `disableReturnData` (default: included) | `enableReturnData` (default: excluded) |

The change is **twofold**:
1. The JSON key is renamed (`disable*` → `enable*`).
2. The default value is inverted: previously memory and return data were **included** by default (opt-out model); now they are **excluded** by default (opt-in model), matching the spec and Geth.

**Migration:**

```jsonc
// Before — disable memory explicitly
{ "disableMemory": true }

// After — enable memory explicitly
{ "enableMemory": true }

// Before — memory included by default (no flag needed)
{}

// After — must opt in
{ "enableMemory": true }
```

Affected RPC methods: `debug_traceTransaction`, `debug_traceBlockByHash`, `debug_traceBlockByNumber`, `debug_traceCall`.

---

## [3.4.3] "Splashing Saga" – TBD

v3.4.3 is a bugfix release recommended for all users.

**Bugfixes**

- db/state: prune `TemporalMemBatch` overlay entries past the unwind point (#21538) by @JkLondon — second
  fix for the post-reorg `gas used mismatch` / state-leak some users still hit on v3.4.2. After a tip reorg
  a stale read in the in-memory overlay could return a write made *inside* the unwound `txNum` range,
  flipping an `SSTORE` from cold to warm gas pricing. Complements the #21157 diffset fix shipped in v3.4.2.
- rpc: match Geth semantics in `debug_getModifiedAccountsByHash` / `debug_getModifiedAccountsByNumber`
  (#21507) by @lupin012 — corrects the block-range convention (exclusive start), now also reports contracts
  whose storage changed without an account change, and excludes touched-but-unchanged precompiles and
  self-destructed accounts.
- node/cli: register `--rpc.logs.maxresults` in `DefaultFlags` so it takes effect via the CLI (#21389) by
  @lupin012 — the limit was documented in 3.4.0 but never wired into the flag set, so setting it on the
  command line had no effect; it now applies.

**Improvements**

- execution/p2p, execution/engineapi: fail-fast `engine_newPayload` backward download when the gap exceeds
  the reorg limit (#21502) by @yperbasis — when a payload's parent is more than `MaxReorgDepth` blocks from
  the local head, the download short-circuits instead of fetching a header batch every slot, and logs the
  expected gap at INFO instead of WARN. The gap is still closed by the following fork-choice update.

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.4.2...v3.4.3

---

## [3.4.2] "Splashing Saga" – 2026-05-22

v3.4.2 is a bugfix release recommended for all users.

**Bugfixes**

- execution/stagedsync: find diffset by actually-executed hash on unwind (#21157) by @JkLondon — fixes a
  state-leak bug in `unwindExec3` that surfaces as `gas used mismatch` / `Cannot update chain head` after
  a tip reorg whose unwound block deployed a contract via `CREATE`/`CREATE2` to a counterfactual address
  (safe-wallets, EIP-1167 clone factories, ERC-4337 accounts, deterministic deployers). The unwind now
  walks every header at the height to find the diffset of the block actually executed, instead of
  assuming the (already-flipped) canonical hash matches.
- rawdb: ignore invalid receipt cache transaction indexes (#21262) by @Sahil-4555

**Improvements**

- db/state, ethconfig: bound domain merge; add `--erigondb.domain.steps-in-frozen-file` (#21148) by
  @wmitsuda

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.4.1...v3.4.2

---

## [3.4.1] "Splashing Saga" – 2026-05-11

**Bugfixes**

- commitment: segfault fix - caused by branch slice returned by TrieContext.Branch (#21044) by @awskii

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.4.0...v3.4.1

---

## [3.4.0] "Splashing Saga" – 2026-04-28

Erigon 3.4.0 is a major update for node operators and validators, focused on stability, performance, and
efficiency at ChainTip. It is a drop-in upgrade for 3.3.x users — no data migration or re-sync required.

### Key Features

- **Fast restart on ChainTip**: no more blocking indexing or pruning at startup; Caplin doesn't lose its
  download progress.
- **4x smaller Chaindata (~20 GB)**: improves ChainTip speed. Available via re-sync, or by running
  `./build/bin/erigon seg step-rebase --datadir=<your_path> --new-step-size=390625` (takes ~10 seconds).
- **Historical `eth_getProof`**: is no longer experimental. Recommended: 32 GB+ RAM. Re-sync with
  `--prune.include-commitment-history` to apply the latest data fixes.
- **New RPC endpoints**: `trace_rawTransaction`, `eth_getStorageValues`, `admin_addTrustedPeer`,
  `admin_removeTrustedPeer`, flat tracers, `engine_getBlobsV3`.
- **Reduced impact on ChainTip performance**: from RPC, background file merging, pruning, and optional
  heavy flags (`--persist.receipts`, `--prune.include-commitment-history`).

### Breaking Changes

- Minimum Go version: 1.25
- `--rpc.blockrange.limit=1_000` new limit. Maximum block range (end - begin) allowed for range queries
  over RPC. 0 - means unlimited. Default: 1_000
- `--rpc.logs.maxresults=20_000` new limit. Maximum number of logs returned by eth_getLogs,
  erigon_getLogs, erigon_getLatestLogs. 0 - means unlimited. Default: 20_000
- `--rpc.max.concurrency=0` new limit. Maximum number of concurrent HTTP RPC requests (HTTP admission
  control). 0 = use db.read.concurrency, -1 = unlimited (no admission control). Default: 0
- p2p: switched to `discv5`. `discv4` disabled by default.

### Added

#### RPC Endpoints

- `trace_rawTransaction`: execute and trace a raw signed transaction without broadcasting it (#19524)
- `eth_getStorageValues`: batch fetch of multiple storage slots in a single call (#19442)
- `admin_addTrustedPeer` / `admin_removeTrustedPeer`: manage trusted peers at runtime (#19413)
- Call flat tracers (`trace_call` family): flat trace output format support (#18556)
- `engine_getBlobsV3`: Engine API blob retrieval v3 (#18512)
- `trace_call`: `StateOverrides` precompile support (#18401, #18492)

#### Consensus & Execution

- Fusaka scheduled for Chiado (Gnosis Chain testnet) at slot 21 651 456, epoch 1 353 216, timestamp
  1 773 653 580 (Mon 16 Mar 2026 09:33:00 UTC) (#19682)
- Chiado bootstrap nodes updated to match the Lighthouse built-in Chiado network config (#19693)
- Balancer hard fork for Gnosis Chain mainnet (#18122)
- Amsterdam signer support and BAL non-determinism fix (#19434)
- BAL selfdestruct net-zero fix (#19528)
- Parallel execution fixes for block-access-list (BAL) workloads (#17319)
- execution/vm: EIP-8024 (`SWAPN`, `DUPN`, `EXCHANGE`) opcodes implemented (#18670)
- Pectra requests-hash validation: fix partial block receipt reconstruction when execution resumes from
  a snapshot boundary mid-block — resolves `invalid requests root hash in header` on mainnet re-sync at
  block 24966723 (#20452)

#### Caplin (Consensus Layer)

- Persistent historical download — Caplin now persists and resumes historical beacon block downloads
  across restarts (#18320)
- Discovery v5 enabled by default — `discv5` is now the default peer discovery protocol (#18578)
- cl/p2p, cl/sentinel: fix DISCV5 ENR missing IP when the discovery address is unspecified (#19585)
- Fix missing attestations by using GossipSub for subnet peer coverage (#19523)
- cl/gossip: fix conditions forwarding, ENR lifecycle, and epoch-mismatch — prevents false peer banning,
  reduces log flooding from redundant ENR updates, and guards against stale RANDAO committee
  computation (#20777)
- cl/beacon: add `single_attestation` event topic support (#18142)
- cl/beacon: set `Eth-Consensus-Version` header when versioned (#18377)

### Changed

#### RPC Reliability

- `eth_getBlockReceipts`: limit over-concurrency — prevents latency growth and out-of-memory (OOM) at
  high request rates (#19725)
- `debug_traceCallMany`: fix global `BlockOverrides` and `StateOverrides` not being applied (#19547)
- `debug_traceCall`: fix state and block overrides interaction (#18480)
- `eth_blobBaseFee`: fix incorrect value returned (#18506)
- `eth_getBalance` and others: fix block-not-found error for certain historical queries (#18457)
- Block-hash canonicality check added to APIs that accept a block hash and state (#19356)
- rpc: fix batch limit exceeded error to comply with the JSON-RPC spec (#18260)
- `trace_replayTransaction()`: add stack info for `TLOAD` opcode (#19550)
- `eth_feeHistory`: performance optimisation and pending block support (#19526, #19455)
- `debug_trace*`: zero-alloc memory word encoding in `JsonStreamLogger` — eliminates per-word heap
  allocations and prevents OOM on large block traces (#20754)
- prestateTracer: fix diff mode missing deleted accounts to match geth behaviour (#20775)
- rpc/mcp: fix `tools/call` hanging indefinitely under DB load by switching the SSE context to
  non-blocking read-tx acquire (#20778)

#### TxPool

- Zombie queued transactions: transactions exceeding `MaxNonceGap` are now evicted (#19449)
- txnprovider/shutter: fix premature encrypted txn pool cleanup and peer drops (#18351)
- txpool: cache `pendingBaseFee` for queue comparisons to reduce recomputation (#18341)

#### P2P

- p2p/sentry: fix wrong OR in case statement for `wit` protocol messages (#19580)
- p2p: better handling of RLP errors in `wit` (#19569)
- discv4 disabled by default on mainnet (discv5 preferred) (#18640)

#### Snapshot & Storage

- merge: prioritize Domain merge over History — 2x less disk space required, and less impact to
  ChainTip from history-merge (#19441)
- merge: fix O(n²) InvertedIndex re-merge — eliminates quadratic time complexity during re-merge
  (#19680)
- `SequenceBuilder`: avoids an intermediate Elias-Fano representation during sequence building and
  merge (#19552, #19567)
- Faster startup: state-file index building deferred to reduce restart latency (#19583, #19407)
- Graceful restart in history download: `SpawnStageHistoryDownload` now honours the stage context, so
  Ctrl+C during history download exits promptly instead of waiting for the download to finish (#20766)
- DB integrity checks: time budget added so checks no longer run unbounded (#20714)
- Commitment-history integrity: skip the integrity check when commitment history is disabled (#20835)
- `seg ls`: report dictionary size and memory usage in the output (#20790)
- anacrolix/torrent: fix peerconn panic on bad reads while serving peer requests (#20748)

### Security

- github.com/buger/jsonparser: bump 1.1.1 → 1.1.2 to address CVE-2026-32285 (HIGH, CVSS 7.5 —
  out-of-bounds read) (#20018)

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.10...v3.4.0

---

## [3.3.10] "Rocky Romp" – 2026-03-27

This release schedules Fusaka on **Gnosis Chain mainnet** at **Tue 14 April 2026, 12:06:20 UTC** and
thus is **mandatory for all Gnosis users**. It is also recommended for all users in general.

**Changes**

- Schedule Gnosis Fusaka (#20090) by @domiwei
- txpool: fully discard pre-Osaka blob txns in best() (#20120) by @yperbasis
- rpc: auto-convert legacy blob sidecar to v1 cell proofs after Osaka (#20087) by @domiwei
- rpc: add blockRangeLimit param for API works on blocks range (#19614) by @lupin012
- cl: fall back to local head state when remote checkpoint sync fails (#20003) by @domiwei
- cl: fix fork choice block validation and error propagation (#19927) by @domiwei
- cl: re-enable voluntary_exit gossip topic subscription (#19764) by @Giulio2002

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.9...v3.3.10

---

## [3.3.9] "Rocky Romp" – 2026-03-09

This release schedules Fusaka on Chiado on **Mon 16 March 2026, 09:33:00 UTC** and thus is mandatory
for all Chiado users.

**Changes**

- fix(txpool): evict zombie queued txns exceeding MaxNonceGap (cherry-pick #19449 → release/3.3)
  (#19591) by @Giulio2002
- cl/sentinel: fix DISCV5 ENR missing IP when discovery address is unspecified (#19647) by @lystopad
- rpc: accept only canonical hash for block receipts (#19649) by @canepat
- Schedule Fusaka for Chiado (#19681) by @yperbasis
- cl: amend Chiado bootstrap nodes (#19692) by @yperbasis

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.8...v3.3.9

---

## [3.3.8] "Rocky Romp" – 2026-02-20

**Changes**

- New Chiado boot nodes (cherry-pick #18867) (#19241) by @sudeepdino008
- eth_getLogs: receipts availability check to be aware about `--persist.receipts` and
  `--prune.mode=minimal` (#19226) by @AskAlexSharov
- txnprovider/shutter: fix decryption keys processing when keys do not follow txnIndex order (#18951)
  (#18959) by @taratorio
- execution: fix Chiado re-exec from genesis (#18887) by @yperbasis

**Bugfixes**

- execution/tests: minor fix chainmaker add withdrawals in shanghai (#18886) by @taratorio
- Reduce impact of background merge/compress to ChainTip (#18995) by @AskAlexSharov
- fix(caplin): Fixes for DataColumnSidecar (#18268) (#19003) by @taratorio
- rpc: bound checks in receipts cache V2 and generator (#19046) by @canepat
- execution/execmodule: fix unwinding logic when side forks go back in height (#18993) (#19063) by
  @taratorio
- p2p: fix nil pointer crash with --nodiscover (#19056) by @sudeepdino008
- rpc: add check on latest executed block (#19133) by @canepat
- protect History from events duplication (#19230) by @AskAlexSharov

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.7...v3.3.8

---

## [3.3.7] "Rocky Romp" – 2026-01-30

P2P stability. Prune performance.

**Changes**

- prune: remove early-exit based on DirtySpace() (#18787) by @AskAlexSharov
- execution: fix commitment state key txNum when last block tx is at step boundary (#18858) by
  @taratorio
- p2p deadlock fix (#18862) by @Copilot
- Forward compatibility: Fixed index building for v0 snapshot format (#18824) by @eastorski
- Backward compatibility: Add --v5disc alias (#18785) by @anacrolix
- Show the default P2P discovery bools in --help (#18819) by @anacrolix

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.4...v3.3.7

---

## [3.3.4] "Rocky Romp" – 2026-01-23

v3.3.4 is a bugfix release recommended for all users.

**Changes**

- Fix for missing rcache at stepBoundary (#18698) by @sudeepdino008
- execution/stagedsync: port --experimental.always-generate-changesets flag for gas repricing
  benchmarks (#18733) by @taratorio
- execution: add --fcu.timeout and --fcu.background.prune flags (#18723) by @taratorio
- Not needed collector erased in prune domains (#18665) by @JkLondon
- Forward compatible snapshots format (#18746) by @eastorski

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.3...v3.3.4

---

## [3.3.3] "Rocky Romp" – 2026-01-14

v3.3.3 is a bugfix release recommended for all users.

**Fixes**

- CL: Fix incorrect committee count in process_attestation (#18316) by @domiwei
- CL: Fix for periodic retry of not-ready response (#18376) by @taratorio
- Shutter: fix premature encrypted txn pool cleanup bug and peer drops (#18264) by @taratorio
- Half block exec fix in receipts (#18426, #18505) by @sudeepdino008
- p2p: resolve security vulnerability (#18650) by @yperbasis

**Improvements**

- p2p: Turn on discovery v5 by default (#18639) by @anacrolix

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.2...v3.3.3

---

## [3.3.2] "Rocky Romp" – 2025-12-13

Gnosis hardfork support.

**Changes**

- rpc: fix panic on `GetEthV1BeaconBlobs` (#17992) (#18232) by @domiwei
- rpc: fix invalid key error code in `eth_getStorageAt` (#18238) by @canepat
- cl/beacon: add single_attestation event topic support (#18142) (#18277) by @domiwei
- Gnosis Balancer (#18282) by @mh0lt
- bittorrent: torrent excessive wantPeers event fix (#18256) by @anacrolix

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.1...v3.3.2

---

## [3.3.1] "Rocky Romp" – 2025-12-07

- We have new Docs and HelpCenter: https://docs.erigon.tech/
- Support of historical `eth_getProof` (https://github.com/erigontech/erigon/issues/12984). It requires
  `--prune.include-commitment-history` flag.

**RPC**

- rpc: fix txpool_content crash: unknown (#18120) by @canepat

**CL**

- Caplin: Simplified peer refreshing (#18123) (#18152) by @domiwei

**TxPool**

- txpool: remove double PopWorst() in pending pool overflow (#18159) by @AskAlexSharov

**Sync**

- Torrent client fixes (#18179) by @anacrolix

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.3.0...v3.3.1

---

## [3.3.0] – 2025-11-27

### Added

- Support of historical `eth_getProof` (https://github.com/erigontech/erigon/issues/12984). It requires
  `--prune.experimental.include-commitment-history` flag.
- Look our new Docs and HelpCenter: https://docs.erigon.tech/

#### RPC Endpoints

- `eth_simulateV1`: Complete implementation of Ethereum simulation API with support for state overrides, blob
  transactions, block overrides, and historical state roots (#15771)
- `eth_createAccessList`: StateOverrides parameter support (#17653)
- Support for `eth_call` with blockOverrides (#17261)
- `trace_filter`: Block tags support (#17238)
- `debug_traceTransaction`: Self-destruct operation validation (EIP 6780) (#17728)

#### Consensus & Execution

- EIP-7928: BlockAccessList type support (#17544)
- EIP-7934: EstimateGas capped by MaxTxnGasLimit in Osaka (#17251)
- EIP-7702 transaction support in `(r *Receipt) decodeTyped` (#17412)
- Rewrite bytecode support for post-Merge blocks (#17770)

#### Caplin (Consensus Layer)

- Get blobs support (Fusaka compatibility) (#17829)

### Changed

#### RPC Improvements

- `eth_getTransactionReceipt`: Pre-Byzantium transaction handling (#17479, #17509)
- `eth_estimateGas`: Improved handling with StateOverrides (#17914, #17295)
- `debug_traceCall`: System contract execution support (#17339)
- Blob transaction and blob base fee override support (#17313)

#### Execution Engine

- Experimental Parallel Exec (#16922)
- MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP for Gnosis (#17501)
- Reduce goroutines amount produced by BitTorrent library (#17765)
- Up base image to `Go 1.25-trixie` (#17837)

### Removed

- PoW mining was removed in #17813. The `--chain=dev` mode now uses an embedded PoS
  consensus layer (Caplin) with deterministic validators instead of Clique. See
  `docs/DEV_CHAIN.md` for usage.
- Holesky network support removed (#17685)
- eth/67 protocol support removed (#17318)
- SkipAnalysis VM optimization removed (#17217)

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.2.2...v3.3.0

---

## [3.2.3] "Quirky Quests" – 2025-11-25

**Changes**

- Caplin: add getBlobs support (fusaka) (#17840) by @Giulio2002

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.2.2...v3.2.3

---

## [3.2.2] "Quirky Quests" – 2025-11-03

v3.2.2 schedules Fusaka on Ethereum mainnet on December 3, 2025 at 09:49:11pm UTC. Thus it is a mandatory update for all Ethereum mainnet users.

**New features**

- Schedule Fusaka on Ethereum mainnet in #17736 by @yperbasis
- Tool to fetch and recover blobs from a remote beacon API in #17611 by @Giulio2002 

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.2.1...v3.2.2

---

## [3.2.1] "Quirky Quests" – 2025-10-20

v3.2.1 is a bugfix release recommended for all users, especially validators.

**Fixes**

- Fix validators producing bad blocks on Hoodi in #17487 by @mh0lt 
- RPC: fix "insufficient funds for gas * price + value" error in traces retrieval for a specific block (Issues #16909, #17232) in #17523 by @antonis19
- RPC: fix no changes and filter not found in eth_getFilter* (Issue #17246) in #17350 by @canepat
- RPC: debug_traceCall fix avoid to trace sysContract (Issue #17220) in #17360 by @lupin012
- CL: fix initial previous_version in fork_schedule (Issue #17262) in #17331 by @domiwei

**Improvements**

- Ethereum mainnet default block gas limit is raised to 60M in #17321 by @yperbasis
- CL: Allow blob requests after Fusaka in #17500 by @domiwei

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.2.0...v3.2.1

---

## [3.2.0] "Quirky Quests" – 2025-10-02

Erigon 3.2.0 has a complete implementation of [Fusaka](https://eips.ethereum.org/EIPS/eip-7607) and schedules it on the
test nets (#17197):

- Holesky on Wednesday, 1 October 2025 08:48:00 UTC
- Sepolia on Tuesday, 14 October 2025 07:36:00 UTC
- Hoodi on Tuesday, 28 October 2025 18:53:12 UTC

**Fixes**

- Re-org/unwind fixes (#17105, #17165) by @taratorio
- RPC: Fixes to eth_getProof (#16220, #16251, #16564, #16606, #16687) by @awskii
- tracer: fix prestates for EIP7702 transactions (#16497) by @nebojsa94

**Improvements**

- New EL block downloader (#16270, #16673) by @taratorio
- Caplin p2p improvements (#16719, #16995) by @Giulio2002
- EVM: MODEXP precompile performance improvements (#16579, #16583, #16396, #17151) by @chfast & @yperbasis
- execution: more accurate bad block responses (#16994) by @taratorio
- Block builder: improve txpool polling (#16412) by @taratorio
- execution/stagedsync: handle sync loop block limit exhaustion (#16268) by @taratorio
- RPC: Apply batch limit to WebSocket/IPC connections (#16255) by @grandchildrice
- RPC: Estimate gas align to geth (#16397) by @lupin012
- snapshotsync: add support for `--snap.download.to.block` (#16938) by @taratorio

**New features**

- Complete Fusaka implementation (#16183, #16185, #16186, #16187, #16184, #16391, #16401, #16207, #16420, #16428,
  #16494, #16457, #16644, #16928, #16945, #17060, #16989, #17076, #17169) by @taratorio, @yperbasis, @Giulio2002 and
  @domiwei
- Implement eth/69 (#15279, #17186, #17171) by @shohamc1
- RPC: implement new eth_config spec (#16218, #16410) by @taratorio
- RPC: impl admin_RemovePeer (#16292) by @lupin012

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.1.0...v3.2.0
 
-----

File following Keep a Changelog spec: https://keepachangelog.com/en/1.1.0/
