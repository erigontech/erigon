ChangeLog
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
