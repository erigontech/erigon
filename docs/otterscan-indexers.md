# Otterscan Indexers Design Document

## Introduction

This document describes the customizations made to support Otterscan optional stages/indexes.

Some changes were made to `erigon-lib`, but they are explained here.

## General conventions

### Database

Every Otterscan owned table is prefixed with `Ots` to keep a clear separation from Erigon core tables.

Their declarations can be found in `erigon-lib`, in `tables.go`.

### Custom indexers

Every Otterscan custom stage is prefixed with `Ots`to keep it clear that it is a custom stage added by Otterscan.

Their declarations can be found in `eth/stagedsync/stages/stages.go`.

## Contract classifiers

### Index tables

Let's first define what is an unique index. For example, all contracts that adhere to the ERC20 standard can be considered an unique index.

In fact, most built-in indexes are attempts to classify deployed contracts into different ERC standards.

For each unique index, there will be a pair of tables `Ots<Name>`/`Ots<Name>Counter`.

The `Ots<Name>` table is a DupSorted table with the following standard structure:

| Key                   | Value                                    |
| --------------------- | ---------------------------------------- |
| `uint64` block number | `[20]byte` contract address + extra data |

That means: the contract at `address` was deployed at `block number` and complies to whatever the indexer that produced this insertion does.

Using `block number` as key provide us a reorg-friendly temporal data structure to undo insertions.

The table pair must be declared in `tables.go` at `erigon-lib` in order to be available for use. There must be a `const` name declaration + whitelisting the table in the `ChaindataTables` array. The non-`Count` table must also be declared as DupSort in `ChaindataTablesCfg` map.

The `Ots<Name>Counter` table is a regular table used for results pagination and it has the following structure:

| Key                          | Value                 |
| ---------------------------- | --------------------- |
| `uint64` accumulated counter | `uint64` block number |

That means: up to `block number` (inclusive), there are `counter` search results.

So if one wants to find the Nth result inside the index (1-based), you must turn `N` into the key and search for it. It'll find the block number the contains the result.

Then get the key of previous record (previous accumulated count), calculate `N` - previous count + 1. The result (0-based) will let you know which match position inside the block corresponds to the desired match.

## Index stages

Each unique index is defined by a custom stage in `eth/stagedsync/ots_indexer_<Name>.go` and a standardized name in `eth/stagedsync/stages/stages.go`.

The custom stage is added to the Erigon stage loop by adding injecting its declaration on `eth/stagedsync/default_stages.go`.

The stages use a custom stage framework so most of common code is abstracted away. More on that later.

Suffice to say that each of these stages operate on an unique index, filtering data from a set of `Ots<Name>/Ots<Name>Counter` tables to its own set. The upper bound block number is also defined by its "parent" stage.

For example, the ERC721 classifier index stage is bound to the ERC165 classifier stage, since every ERC721 must implement ERC165, so we reduce significantly the universe of contracts to be analyzed by making it a child of ERC165 stage.

As an utility "hack", a "reset" command to cleanup tables should be created in `cmd/integration/commands/stages.go` along with reset instructions in `core/rawdb/rawdbreset/reset_stages.go`, `Tables` mapping.

## Benckmarks

Reference hardware: gcloud n2-standard-8

### Total disk space (27/08/2023)

Baseline mainnet:

wmitsuda@mainnet-wmitsuda-2:/mnt/erigon$ du -hd1 .
410G    ./snapshots
16K     ./lost+found
188M    ./txpool
195M    ./logs
21M     ./nodes
1.4M    ./temp
1.9T    ./chaindata
2.3T    .

After:

wmitsuda@mainnet-wmitsuda-2:/mnt/erigon$ du -hd1 .
412G    ./snapshots
16K     ./lost+found
291M    ./txpool
211M    ./logs
22M     ./nodes
1.6M    ./temp
1.9T    ./chaindata
2.3T    .

### All contracts (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|09:22:02.856] [15/24 OtsContractIndexer] Finished      latest=18080701
[INFO] [09-07|09:22:05.706] [15/24 OtsContractIndexer] DONE          in=1h57m7.535823984s
```

Space: (2.7GB + 190MB == 2.9GB)

```
Status of OtsAllContracts
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 79322
  Leaf pages: 585383
  Overflow pages: 0
  Entries: 57806584
Status of OtsAllContractsCounter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 206
  Leaf pages: 46120
  Overflow pages: 0
  Entries: 7194585
```

### ERC20 (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|10:32:21.222] [16/24 OtsERC20Indexer] Totals           totalMatches=898829 totalProbed=57785855
[INFO] [09-07|10:32:21.222] [16/24 OtsERC20Indexer] Finished         latest=18080701
[INFO] [09-07|10:32:21.584] [16/24 OtsERC20Indexer] DONE             in=1h10m15.877153125s
```

Space: (48MB + 19MB == 67MB)

```
Status of OtsERC20
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 58
  Leaf pages: 11807
  Overflow pages: 0
  Entries: 899283
Status of OtsERC20Counter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 22
  Leaf pages: 4687
  Overflow pages: 0
  Entries: 731079
```

### ERC165 (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|11:42:16.335] [17/24 OtsERC165Indexer] Totals          totalMatches=3806792 totalProbed=57785855
[INFO] [09-07|11:42:16.335] [17/24 OtsERC165Indexer] Finished        latest=18080701
[INFO] [09-07|11:42:17.734] [17/24 OtsERC165Indexer] DONE            in=1h9m56.149619591s
```

Space: (150MB + 17MB == 167MB)

```
Status of OtsERC165
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 345
  Leaf pages: 36187
  Overflow pages: 0
  Entries: 3809924
Status of OtsERC165Counter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 20
  Leaf pages: 4167
  Overflow pages: 0
  Entries: 650032
```

### ERC721 (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|11:49:04.381] [18/24 OtsERC721Indexer] Totals          totalMatches=3314490 totalProbed=3806792
[INFO] [09-07|11:49:04.381] [18/24 OtsERC721Indexer] Finished        latest=18080701
[INFO] [09-07|11:49:05.778] [18/24 OtsERC721Indexer] DONE            in=6m48.043524943s
```

Space: (128MB + 8MB == 136MB)

```
Status of OtsERC721
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 274
  Leaf pages: 30897
  Overflow pages: 0
  Entries: 3317317
Status of OtsERC721Counter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 10
  Leaf pages: 1939
  Overflow pages: 0
  Entries: 302365
```

### ERC1155 (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|11:54:51.061] [19/24 OtsERC1155Indexer] Totals         totalMatches=49185 totalProbed=3806792
[INFO] [09-07|11:54:51.061] [19/24 OtsERC1155Indexer] Finished       latest=18080701
[INFO] [09-07|11:54:51.649] [19/24 OtsERC1155Indexer] DONE           in=5m45.871004363s
```

Space: (2.1MB + 1MB == 3.1MB)

```
Status of OtsERC1155
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 4
  Leaf pages: 522
  Overflow pages: 0
  Entries: 49198
Status of OtsERC1155Counter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 3
  Leaf pages: 257
  Overflow pages: 0
  Entries: 39954
```

### ERC1167 (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|12:42:05.034] [20/24 OtsERC1167Indexer] Totals         totalMatches=13690895 totalProbed=57785855
[INFO] [09-07|12:42:05.034] [20/24 OtsERC1167Indexer] Finished       latest=18080701
[INFO] [09-07|12:42:08.327] [20/24 OtsERC1167Indexer] DONE           in=47m16.678467963s
```

Space: (598MB + 46MB == 644MB)

```
Status of OtsERC1167
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 3706
  Leaf pages: 142144
  Overflow pages: 0
  Entries: 13709968
Status of OtsERC1167Counter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 51
  Leaf pages: 11222
  Overflow pages: 0
  Entries: 1750617
```

### ERC4626 (08/09/2023)

Time (15 workers):

```
[INFO] [09-07|12:44:33.584] [21/24 OtsERC4626Indexer] Totals         totalMatches=1478 totalProbed=898829
[INFO] [09-07|12:44:33.584] [21/24 OtsERC4626Indexer] Finished       latest=18080701
[INFO] [09-07|12:44:33.616] [21/24 OtsERC4626Indexer] DONE           in=2m25.288537184s
```

Space: (82KB + 37KB == 119KB)

```
Status of OtsERC4626
  Pagesize: 4096
  Tree depth: 2
  Branch pages: 1
  Leaf pages: 18
  Overflow pages: 0
  Entries: 1479
Status of OtsERC4626Counter
  Pagesize: 4096
  Tree depth: 2
  Branch pages: 1
  Leaf pages: 9
  Overflow pages: 0
  Entries: 1256
```

### ERC20+721 Transfers (08/09/2023)

Time:

```
[INFO] [09-07|23:12:03.800] [22/24 OtsERC20And721Transfers] Totals   matches=852510711 txCount=1154620749
[INFO] [09-07|23:12:03.803] [22/24 OtsERC20And721Transfers] Finished latest=18080701
[INFO] [09-07|23:12:07.749] [22/24 OtsERC20And721Transfers] DONE     in=10h27m34.132621965s
```

Space: (5.7GB + 23GB + 332MB + 2.3GB == 31.33GB)

```
Status of OtsERC20TransferCounter
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 12936
  Leaf pages: 1388737
  Overflow pages: 0
  Entries: 137270901
Status of OtsERC20TransferIndex
  Pagesize: 4096
  Tree depth: 5
  Branch pages: 82323
  Leaf pages: 5592031
  Overflow pages: 12994
  Entries: 137270901
Status of OtsERC721TransferCounter
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 649
  Leaf pages: 80444
  Overflow pages: 0
  Entries: 8201734
Status of OtsERC721TransferIndex
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 6654
  Leaf pages: 551828
  Overflow pages: 2676
  Entries: 8201734
```

### ERC20+721 Holders (08/09/2023)

Time:

```
[INFO] [09-08|07:36:38.849] [23/24 OtsERC20And721Holdings] Totals    matches=852510711 txCount=1154620749
[INFO] [09-08|07:36:38.850] [23/24 OtsERC20And721Holdings] Finished  latest=18080701
[INFO] [09-08|07:36:42.616] [23/24 OtsERC20And721Holdings] DONE      in=8h24m34.86630761s
```

Space: (17GB + 3GB == 20GB)

```
Status of OtsERC20Holdings
  Pagesize: 4096
  Tree depth: 5
  Branch pages: 148710
  Leaf pages: 4015927
  Overflow pages: 0
  Entries: 293188023

Status of OtsERC721Holdings
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 73097
  Leaf pages: 681735
  Overflow pages: 0
  Entries: 53484559
```

### Withdrawals (26/09/2023)

Time:

```
[INFO] [09-27|00:37:38.379] [21/22 OtsWithdrawals] Totals            matches=1188617 blocks=18223557
[INFO] [09-27|00:37:38.380] [21/22 OtsWithdrawals] Finished          latest=18223556
[INFO] [09-27|00:37:38.821] [21/22 OtsWithdrawals] DONE              in=2m0.81539878s
```

Space (31.5MB + 4.3MB + 168.3MB == 204.1MB)

```
Status of OtsWithdrawalIdx2Block
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 36
  Leaf pages: 7655
  Overflow pages: 0
  Entries: 1194115
Status of OtsWithdrawalsCounter
  Pagesize: 4096
  Tree depth: 3
  Branch pages: 46
  Leaf pages: 1017
  Overflow pages: 0
  Entries: 126758
Status of OtsWithdrawalsIndex
  Pagesize: 4096
  Tree depth: 4
  Branch pages: 505
  Leaf pages: 40587
  Overflow pages: 34
  Entries: 126758
```
