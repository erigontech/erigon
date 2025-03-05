ChangeLog
---------

## v2.61.3

**Bugfixes:**
* Pectra: fix bad deposit contract deposit unmarshalling by @Giulio2002 in https://github.com/erigontech/erigon/pull/14074

## v2.61.2

**Improvements:**
* Engine API: shorter waits by @yperbasis in https://github.com/erigontech/erigon/pull/13821
* Schedule Pectra for Chiado by @yperbasis in https://github.com/erigontech/erigon/pull/13935

**Bugfixes:**
* rpcdaemon: Don't set miner by @shohamc1 in https://github.com/erigontech/erigon/pull/13799

## v2.61.1 

**Breaking changes:**

- Prohibit --internalcl in E2 by @yperbasis in https://github.com/erigontech/erigon/pull/13757

**New Feature:**

- Up to date implementation of EIPs for Pectra and scheduled hard fork for Sepolia and Holešky
- Add support to eth_blobBaseFee, eth_baseFee and EIP4844 support to eth_feeHistory
- Add support for `engine_getClientVersionV1`

**Bugfixes:**

- rpcdaemon: Set miner on eth_getBlockByNumber on Polygon by @shohamc1 in https://github.com/erigontech/erigon/pull/13336
- Fix incorrect intrinsic gas calculation by @shohamc1 in https://github.com/erigontech/erigon/pull/13632


## v2.61.0

**Improvements:**

- Up to date implementation of Pectra network fork specs till [pectra-devnet-4](https://notes.ethereum.org/@ethpandaops/pectra-devnet-4)
(compatible with the [Mekong testnet](https://blog.ethereum.org/2024/11/07/introducing-mekong-testnet)).
See https://eips.ethereum.org/EIPS/eip-7600
- Updated golang version for Dockerfile 1.22

**Bugfixes:**

- Fix trace_block returning "insufficient funds" (Issues [#12525](https://github.com/erigontech/erigon/issues/12525) and similar) with standalone rpcdaemon by @yperbasis in [#13128](https://github.com/erigontech/erigon/issues/13128)


## v2.60.10

**Bugfixes:**

- Trace API: commit state changes from InitializeBlockExecution @yperbasis in [#12559](https://github.com/erigontech/erigon/pull/12559).
Prior to this PR in `callManyTransactions` (invoked by `trace_block`)
changes made by `InitializeBlockExecution` were discarded. That was immaterial before since no much was
happening at the beginning of a block. But that changed in Dencun with
[EIP-4788](https://eips.ethereum.org/EIPS/eip-4788).
Fixes Issues 
[#11871](https://github.com/erigontech/erigon/issues/11871),
[#12092](https://github.com/erigontech/erigon/issues/12092),
[#12242](https://github.com/erigontech/erigon/issues/12242),
[#12432](https://github.com/erigontech/erigon/issues/12432),
[#12473](https://github.com/erigontech/erigon/issues/12473),
and [#12525](https://github.com/erigontech/erigon/issues/12525).
