# Mainnet MPT to PBT conversion: measured cost

Full conversion of mainnet state from erigon's hex commitment to the
[EIP-8297](https://eips.ethereum.org/EIPS/eip-8297) partitioned binary tree, run to
completion on one machine. This records what it cost so other teams can reason about
migration cost without repeating the run. Every figure below is measured on the run
described here. Nothing is extrapolated, and the closing section says which figures do
not generalise.

## What was converted

| | |
|---|---|
| chain | mainnet |
| anchor | block 25743399, txNum 3,721,093,749 (step 9526) step_size=390625|
| target | `bin-patricia-hashed`, blake3 |
| final root | `ac1aad1f5dfa33083c8d5681eca76916aa1e96c16bc20aa1dcd6e0eab5094364` |

## Machine

- AMD EPYC 4344P, 8 cores / 16 threads, one socket, one NUMA node
- 125.4 GiB RAM (131,501,796 kB), no swap configured
- RAID0 over 2x Samsung MZQL27T6HBLA 7 TB U.2 NVMe, 14 TB
- Linux 6.8.0-101-generic

## Shard cost variance

Shards carry equal key counts and do not carry equal cost.

| | range 0-8192 | range 8192-9216 |
|---|---|---|
| shards | 64 | 8 |
| keys per shard | 27.99M | 38.56M |
| min compute | 4m38s | 18m43s |
| median compute | 54m07s | — |
| max compute | 2h29m17s | 4h36m24s |
| spread | 32x | 15x |

| run  keys | wall |
|---|---|---|
| **total** | **2.20G** | **109h13m** (4.55 days) |

In range `0-8192` the cost grows monotonically with shard index: the first five shards
computed in 4m47 / 5m26 / 6m59 / 6m34 / 4m38, the last five in 2h08 / 2h28 / 2h10 /
2h18 / 2h29. Each shard folds against the tree its predecessors have already built.
Sizing parallel conversion work by key count will not balance it.

## Memory

| | |
|---|---|
| peak RSS (`VmHWM`) | 62.43 GiB |
| steady-state RSS | ~27 GB |

`VmHWM` counts mapped file pages, which is most of the gap between the peak and the
steady state.

## Sizes

| domain | files | size |
|---|---|---|
| accounts | 7 | 16.00 GB |
| storage | 7 | 84.98 GB |
| code | 7 | 18.42 GB |
| **state total** | | **119.40 GB** |
| commitment, hex | 7 | 286.34 GB |
| commitment, bin | 7 | 430.37 GB |
| bin `.kvi` accessors | 7 | 22.43 GB |

The bin/hex multiplier is 1.503x overall and **inverts with range age**. It is a property
of state density, not of the binary structure, so "binary costs 1.5x" is not a
transferable claim.

| range | hex | bin | bin/hex | bin records |
|---|---|---|---|---|
| 0-8192 | 180.47 GB | 320.07 GB | 1.773x | 2,530,448,382 |
| 8192-9216 | 60.62 GB | 73.91 GB | 1.219x | 658,227,540 |
| 9216-9472 | 30.53 GB | 26.07 GB | 0.854x | 248,864,141 |
| 9472-9504 | 8.01 GB | 5.25 GB | 0.655x | 53,904,798 |
| 9504-9520 | 4.45 GB | 3.47 GB | 0.780x | 31,060,897 |
| 9520-9524 | 1.41 GB | 1.01 GB | 0.716x | 9,482,352 |
| 9524-9526 | 0.85 GB | 0.59 GB | 0.697x | 5,606,345 |
| **total** | **286.34 GB** | **430.37 GB** | **1.503x** | **3,537,594,455** |

Record counts are seg word counts halved (each record is a key word and a value word).
Across all seven files that is 121.7 bytes per record on disk, key and value, after
compression.

## Commands

**The trie variant is not a command-line argument.** It is persisted state, written by the rebuild
into the output datadir's `snapshots/erigondb.toml` and read back on every later start:

```toml
step_size = 390625
steps_in_frozen_file = 256
references_in_commitment_branches = false
trie_variant = 'bin'
trie_hash = 'blake3'
```

The source datadir's own `erigondb.toml` carries neither trie key, which is how it stays hex.
Anyone reproducing this needs to know the variant travels with the datadir, not the command.

```
 ~/erigon/build/bin/integration commitment rebuild \
  --chain=mainnet \
  --datadir=/erigon-data/mainnet-caplin \
  --output.datadir=/erigon-data/bin-trie \
  --experimental.bin-commitment \
  --experimental.bin-commitment.hash=blake3 \
  --squeeze=false \
  --no-history \
  --yes
```
