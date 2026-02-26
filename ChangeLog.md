# Changelog

## [3.3.0] – 2025-11-17

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

- PoW mining was removed in #17813, which resulted in `--chain=dev` not being able to produce new blocks. Going forward
  we'll either sunset `--chain=dev` or switch it to mock CL (see #14753). If you need `--chain=dev`, please use Erigon
  3.2
- Holesky network support removed (#17685)
- eth/67 protocol support removed (#17318)
- SkipAnalysis VM optimization removed (#17217)

**Full Changelog**: https://github.com/erigontech/erigon/compare/v3.2.2...v3.3.0

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
