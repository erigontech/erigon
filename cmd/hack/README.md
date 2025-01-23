# Hack

Hack is a set of developer-focused tools for dealing with the node and its data.

## Tools

link to other tools READMEs

- [Hack](#hackgo)
- [Allocs](allocs/README.md)
- [RPC Cache](rpc_cache/README.md)
- [RPC Checker](rpc_checker/README.md)
- [Scripts](scripts/README.md) collection of scripts.
- [Zk Debug Tools](../../zk/debug_tools/README.md) another set of tools in this repository.

## Developer Flags
- [Debug Flags](debug/README.md) - for limiting the node block height

## Hack.go
This program is a command-line tool for various blockchain-related operations. It supports multiple actions that can be specified via command-line flags. Below is a list of supported actions and their descriptions:

### Usage

you call the folder and not the file hack.go also pay attention to chaindata flag description.

```sh
hack [flags]
```

### Flags

| Flag           | Description                                                                                               |
|----------------|-----------------------------------------------------------------------------------------------------------|
| `-action`      | Specifies the action to perform. Supported actions are listed below.                                      |
| `-cpuprofile`  | Specifies the file to write proof CPU profile data to.                                                    |
| `-chaindata`   | Specifies the path to the chain data. ( this is not datadir root, you have to specify chaindata folder ). |
| `-output`      | Specifies the output file.                                                                                |
| `-block`       | Specifies the block number.                                                                               |
| `-blockTotal`  | Specifies the total number of blocks.                                                                     |
| `-hash`        | Specifies the hash value.                                                                                 |
| `-account`     | Specifies the account address.                                                                            |
| `-bucket`      | Specifies the bucket name.                                                                                |
| `-name`        | Specifies the name.                                                                                       |
| `-cfglocation` | Specifies the location where the dynamic config lies for zkCfgMerge.                                      |
| `-chain`       | Specifies the chain name for zkCfgMerge. Should the on the filenames of the dynamic config.               |

### Supported Actions

| Action                   | Description                                           |
|--------------------------|-------------------------------------------------------|
| `cfg`                    | Generate a test configuration.                        |
| `defrag`                 | Defragment the database.                              |
| `textInfo`               | Get text information about the database.              |
| `fixTd`                  | Fix total difficulty.                                 |
| `fixState`               | Fix the state.                                        |
| `advanceExec`            | Advance execution.                                    |
| `backExec`               | Rollback execution.                                   |
| `extractCode`            | Extract code from the database.                       |
| `iterateOverCode`        | Iterate over code in the database.                    |
| `nextIncarnation`        | Get the next incarnation of an account.               |
| `dumpStorage`            | Dump storage data.                                    |
| `countAccounts`          | Count the number of accounts.                         |
| `extractBodies`          | Extract block bodies.                                 |
| `repairCurrent`          | Repair the current state.                             |
| `snapSizes`              | Get snapshot sizes.                                   |
| `current`                | Print the current block number.                       |
| `bucket`                 | Print a specific bucket.                              |
| `buckets`                | Print all buckets.                                    |
| `slice`                  | Slice the database.                                   |
| `searchChangeSet`        | Search the change set.                                |
| `readCallTraces`         | Read call traces of a block                           |
| `extractHeaders`         | Extract block headers.                                |
| `extractHashes`          | Extract block hashes.                                 |
| `printTxHashes`          | Print transaction hashes.                             |
| `testBlockHashes`        | Test block hashes.( needs hash and block )            |
| `readAccount`            | Read account information.                             |
| `readAccountAtVersion`   | Read account information at a specific block.         |
| `searchStorageChangeSet` | Search the storage change set. (needs hash and block) |
| `trimTxs`                | Trim transactions.                                    |
| `scanTxs`                | Scan transactions.                                    |
| `scanReceipts2`          | Scan receipts (method 2).                             |
| `scanReceipts3`          | Scan receipts (method 3). (use a block)               |
| `devTx`                  | Perform a development transaction.                    |
| `chainConfig`            | Get chain configuration.                              |
| `findPrefix`             | Find a prefix in the database.                        |
| `findLogs`               | Find logs in a block range.                           |
| `iterate`                | Iterate over the database.                            |
| `rmSnKey`                | Remove a specific key.                                |
| `readSeg`                | Read a segment.                                       |
| `dumpState`              | Dump the state.                                       |
| `getOldAccInputHash`     | Get the old account input hash.                       |
| `dumpAll`                | Dump all data to an output file.                      |
| `zkCfgMerge`               | Merge dynamic config files into 1 file.               |

### Examples
```sh
.cmd/hack -action=cfg
.cmd/hack -action=testBlockHashes -chaindata=/path/to/chaindata -block=12345 -hash=0xabc...
.cmd/hack -action=readAccount -chaindata=/path/to/chaindata -account=0xabc...
.cmd/hack -action=extractHeaders -chaindata=/path/to/chaindata -block=12345 -blockTotal=100
.cmd/hack -action=dumpAll -chaindata=/path/to/chaindata -output=/path/to/output
.cmd/hack -action=zkCfgMerge -cfglocation=/config/location -chain=mychain-testnet -output=/path/to/output.json
```

### Note
- Ensure that the required flags for each action are provided.
- if using `-cpuprofile` flag The program also starts a cpuprofile on file with pprof package.

```sh
.cmd/hack —action=buckets —chaindata="/Users/afa/Code/gateway/erigon-data/bali-2"
```

