---
description: 'Erigon RPC debug Namespace: Deep Diagnostics and State Introspection'
---

# debug

The `debug` namespace provides debugging and diagnostic methods for Erigon node operators and developers. These methods offer deep introspection into blockchain state, transaction execution, and node performance. The debug namespace is implemented through the `PrivateDebugAPI` interface and `DebugAPIImpl` struct.

The debug namespace must be explicitly enabled using the `--http.api` flag when starting the RPC daemon. For security reasons, these methods are considered private and should not be exposed on public RPC endpoints.

### Security and Access Control

* Debug methods are considered private and should not be exposed on public RPC endpoints;
* These methods can consume significant resources and should be used carefully in production environments;
* Access should be restricted to trusted operators and developers only.

### Performance Considerations

* Tracing methods (`debug_traceTransaction`, `debug_traceBlockByHash`, etc.) support streaming to handle large results efficiently;
* The `AccountRangeMaxResults` constant limits account range queries to 8192 results, or 256 when storage is included;
* Memory and GC control methods allow fine-tuning of node performance.

### Integration with Erigon Architecture

* Debug methods leverage Erigon's temporal database for historical state access;
* The implementation uses `kv.TemporalRoDB` for efficient historical queries;
* Tracing functionality integrates with Erigon's execution engine and EVM implementation.

### Usage in Development and Testing

* These methods are essential for debugging transaction execution issues;
* Storage range methods help analyze contract state changes;
* Memory management methods assist in performance optimization and resource monitoring.


# JSON RPC API Reference



## `debug_accountRange`

Enumerates all accounts at a given block with paging capability. `maxResults` are returned in the page and the items have keys that come after the `start` key (hashed address).

- **Parameters:**
  - `blockNrOrHash`: The block number or hash.
  - `start`: The starting key (hashed address).
  - `maxResults`: The maximum number of results to return.
  - `nocode`: (Optional) Whether to exclude code.
  - `nostorage`: (Optional) Whether to exclude storage.
  - `incompletes`: (Optional) Whether to include incomplete accounts.

- **RPC Call:**
  ```json
  RPC{"method": "debug_accountRange", "params": [blockNrOrHash, start, maxResults, nocode, nostorage, incompletes]}
  ```

## `debug_backtraceAt`

Sets the logging backtrace location. When a backtrace location is set and a log message is emitted at that location, the stack of the goroutine executing the log statement will be printed to stderr.

- **Parameters:**
  - `string`: The location specified as `<filename>:<line>`.

- **RPC Call:**
  ```json
  RPC{"method": "debug_backtraceAt", "params": [string]}
  ```

- **Example:**
  ```json
  debug.backtraceAt("server.go:443")
  ```

## `debug_blockProfile`

Turns on block profiling for the given duration and writes profile data to disk. It uses a profile rate of 1 for most accurate information. If a different rate is desired, set the rate and write the profile manually using `debug_writeBlockProfile`.

- **Parameters:**
  - `file`: The file to write the profile data to.
  - `seconds`: The duration to run the profile.

- **RPC Call:**
  ```json
  RPC{"method": "debug_blockProfile", "params": [string, number]}
  ```

## `debug_chaindbCompact`

Flattens the entire key-value database into a single level, removing all unused slots and merging all keys.

- **RPC Call:**
  ```json
  RPC{"method": "debug_chaindbCompact", "params": []}
  ```

## `debug_chaindbProperty`

Returns leveldb properties of the key-value database.

- **Parameters:**
  - `property`: The property to retrieve.

- **RPC Call:**
  ```json
  RPC{"method": "debug_chaindbProperty", "params": [property]}
  ```

## `debug_cpuProfile`

Turns on CPU profiling for the given duration and writes profile data to disk.

- **Parameters:**
  - `file`: The file to write the profile data to.
  - `seconds`: The duration to run the profile.

- **RPC Call:**
  ```json
  RPC{"method": "debug_cpuProfile", "params": [string, number]}
  ```

## `debug_dbAncient`

Retrieves an ancient binary blob from the freezer. The freezer is a collection of append-only immutable files. The first argument `kind` specifies which table to look up data from.

- **Parameters:**
  - `kind`: The table kind (e.g., headers, hashes, bodies, receipts, diffs).
  - `number`: The block number.

- **RPC Call:**
  ```json
  RPC{"method": "debug_dbAncient", "params": [string, number]}
  ```

## `debug_dbAncients`

Returns the number of ancient items in the ancient store.

- **RPC Call:**
  ```json
  RPC{"method": "debug_dbAncients", "params": []}
  ```

## `debug_dbGet`

Returns the raw value of a key stored in the database.

- **Parameters:**
  - `key`: The key to retrieve.

- **RPC Call:**
  ```json
  RPC{"method": "debug_dbGet", "params": [key]}
  ```

## `debug_dumpBlock`

Retrieves the state that corresponds to the block number and returns a list of accounts (including storage and code).

- **Parameters:**
  - `number`: The block number.

- **RPC Call:**
  ```json
  RPC{"method": "debug_dumpBlock", "params": [number]}
  ```

- **Example:**
  ```json
  > debug.dumpBlock(10)
  {
    fff7ac99c8e4feb60c9750054bdc14ce1857f181: {
      balance: "49358640978154672",
      code: "",
      codeHash: "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
      nonce: 2,
      root: "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
      storage: {}
    },
    fffbca3a38c3c5fcb3adbb8e63c04c3e629aafce: {
      balance: "3460945928",
      code: "",
      codeHash: "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
      nonce: 657,
      root: "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
      storage: {}
    }
  },
  root: "19f4ed94e188dd9c7eb04226bd240fa6b449401a6c656d6d2816a87ccaf206f1"
  }
  ```

## `debug_freeOSMemory`

Forces garbage collection.

- **RPC Call:**
  ```json
  RPC{"method": "debug_freeOSMemory", "params": []}
  ```

## `debug_freezeClient`

Forces a temporary client freeze, normally when the server is overloaded. Available as part of LES light server.

- **Parameters:**
  - `node`: The node to freeze.

- **RPC Call:**
  ```json
  RPC{"method": "debug_freezeClient", "params": [node]}
  ```

## `debug_gcStats`

Returns garbage collection statistics. See <https://golang.org/pkg/runtime/debug/#GCStats> for information about the fields of the returned object.

- **RPC Call:**
  ```json
  RPC{"method": "debug_gcStats", "params": []}
  ```

## `debug_getAccessibleState`

Returns the first number where the node has accessible state on disk. This is the post-state of that block and the pre-state of the next block. The `(from, to)` parameters are the sequence of blocks to search, which can go either forwards or backwards.

- **Parameters:**
  - `from`: The starting block number.
  - `to`: The ending block number.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getAccessibleState", "params": [from, to]}
  ```

## `debug_getBadBlocks`

Returns a list of the last 'bad blocks' that the client has seen on the network and returns them as a JSON list of block-hashes.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getBadBlocks", "params": []}
  ```

## `debug_getRawBlock`

Retrieves and returns the RLP encoded block by number.

- **Parameters:**
  - `blockNrOrHash`: The block number or hash.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getRawBlock", "params": [blockNrOrHash]}
  ```

## `debug_getRawHeader`

Returns an RLP-encoded header.

- **Parameters:**
  - `blockNrOrHash`: The block number or hash.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getRawHeader", "params": [blockNrOrHash]}
  ```

## `debug_getRawTransaction`

Returns the bytes of the transaction.

- **Parameters:**
  - `transactionHash`: The transaction hash.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getRawTransaction", "params": [transactionHash]}
  ```

## `debug_getModifiedAccountsByHash`

Returns all accounts that have changed between the two blocks specified. A change is defined as a difference in nonce, balance, code hash, or storage hash. With one parameter, returns the list of accounts modified in the specified block.

- **Parameters:**
  - `startHash`: The starting block hash.
  - `endHash`: The ending block hash.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getModifiedAccountsByHash", "params": [startHash, endHash]}
  ```

## `debug_getModifiedAccountsByNumber`

Returns all accounts that have changed between the two blocks specified. A change is defined as a difference in nonce, balance, code hash or storage hash.

- **Parameters:**
  - `startNum`: The starting block number.
  - `endNum`: The ending block number.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getModifiedAccountsByNumber", "params": [startNum, endNum]}
  ```

## `debug_getRawReceipts`

Returns the consensus-encoding of all receipts in a single block.

- **Parameters:**
  - `blockNrOrHash`: The block number or hash.

- **RPC Call:**
  ```json
  RPC{"method": "debug_getRawReceipts", "params": [blockNrOrHash]}
  ```

## `debug_goTrace`

Turns on Go runtime tracing for the given duration and writes trace data to disk.

- **Parameters:**
  - `file`: The file to write the trace data to.
  - `seconds`: The duration to run the trace.

- **RPC Call:**
  ```json
  RPC{"method": "debug_goTrace", "params": [string, number]}
  ```

## `debug_intermediateRoots`

Executes a block (bad- or canon- or side-) and returns a list of intermediate roots: the stateroot after each transaction.

- **Parameters:**
  - `blockHash`: The block hash.
  - `options`: Additional options.

- **RPC Call:**
  ```json
  RPC{"method": "debug_intermediateRoots", "params": [blockHash, {}]}
  ```

## `debug_memStats`

Returns detailed runtime memory statistics.

- **RPC Call:**
  ```json
  RPC{"method": "debug_memStats", "params": []}
  ```

## `debug_mutexProfile`

Turns on mutex profiling for `nsec` seconds and writes profile data to file. It uses a profile rate of 1 for most accurate information. If a different rate is desired, set the rate and write the profile manually.

- **Parameters:**
  - `file`: The file to write the profile data to.
  - `nsec`:

## `debug_preimage`

Returns the preimage for a sha3 hash, if known.

- **Client Method invocation:**
  - **Console:** `debug.preimage(hash)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_preimage", "params": [hash]}
  ```

## `debug_printBlock`

Retrieves a block and returns its pretty printed form.

- **Client Method invocation:**
  - **Console:** `debug.printBlock(number uint64)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_printBlock", "params": [number]}
  ```

## `debug_setBlockProfileRate`

Sets the rate (in samples/sec) of goroutine block profile data collection. A non-zero rate enables block profiling, setting it to zero stops the profile. Collected profile data can be written using `debug_writeBlockProfile`.

- **Client Method invocation:**
  - **Console:** `debug.setBlockProfileRate(rate)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_setBlockProfileRate", "params": [number]}
  ```

## `debug_setGCPercent`

Sets the garbage collection target percentage. A negative value disables garbage collection.

- **Client Method invocation:**
  - **Go:** `debug.SetGCPercent(v int)`
  - **Console:** `debug.setGCPercent(v)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_setGCPercent", "params": [v]}
  ```

## `debug_setHead`

Sets the current head of the local chain by block number. Note, this is a destructive action and may severely damage your chain. Use with extreme caution.

- **Client Method invocation:**
  - **Go:** `debug.SetHead(number uint64)`
  - **Console:** `debug.setHead(number)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_setHead", "params": [number]}
  ```
- **References:** [Ethash](https://ethereum.org/en/developers/docs/consensus-mechanisms/ethash/)

## `debug_setMutexProfileFraction`

Sets the rate of mutex profiling.

- **Client Method invocation:**
  - **Console:** `debug.setMutexProfileFraction(rate int)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_setMutexProfileFraction", "params": [rate]}
  ```

## `debug_setTrieFlushInterval`

Configures how often in-memory state tries are persisted to disk. The interval needs to be in a format parsable by a `time.Duration`. Note that the interval is not wall-clock time. Rather it is accumulated block processing time after which the state should be flushed. For example, the value `0s` will essentially turn on archive mode. If set to `1h`, it means that after one hour of effective block processing time, the trie would be flushed. If one block takes 200ms, a flush would occur every `5*3600=18000` blocks. The default interval for mainnet is `1h`.

Note: this configuration will not be persisted through restarts.

- **Client Method invocation:**
  - **Console:** `debug.setTrieFlushInterval(interval string)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_setTrieFlushInterval", "params": [interval]}
  ```

## `debug_stacks`

Returns a printed representation of the stacks of all goroutines. Note that the web3 wrapper for this method takes care of the printing and does not return the string.

- **Client Method invocation:**
  - **Console:** `debug.stacks(filter *string)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_stacks", "params": [filter]}
  ```

## `debug_standardTraceBlockToFile`

When JS-based tracing (see below) was first implemented, the intended use case was to enable long-running tracers that could stream results back via a subscription channel. This method works a bit differently. (For full details, see PR)

It streams output to disk during the execution, to not blow up the memory usage on the node
It uses `jsonl` as output format (to allow streaming)
Uses a cross-client standardized output, so called 'standard json'
Uses `op` for string-representation of opcode, instead of `op/opName` for numeric/string, and other similar small differences.
has `refund`
Represents memory as a contiguous chunk of data, as opposed to a list of 32-byte segments like `debug_traceTransaction`
This means that this method is only 'useful' for callers who control the node -- at least sufficiently to be able to read the artefacts from the filesystem after the fact.

The method can be used to dump a certain transaction out of a given block:

```json
debug.standardTraceBlockToFile("0x0bbe9f1484668a2bf159c63f0cf556ed8c8282f99e3ffdb03ad2175a863bca63", {txHash:"0x4049f61ffbb0747bb88dc1c85dd6686ebf225a3c10c282c45a8e0c644739f7e9", disableMemory:true})
["/tmp/block_0x0bbe9f14-14-0x4049f61f-099048234"]
```

Or all txs from a block:

```json
debug.standardTraceBlockToFile("0x0bbe9f1484668a2bf159c63f0cf556ed8c8282f99e3ffdb03ad2175a863bca63", {disableMemory:true})
["/tmp/block_0x0bbe9f14-0-0xb4502ea7-409046657", "/tmp/block_0x0bbe9f14-1-0xe839be8f-954614764", "/tmp/block_0x0bbe9f14-2-0xc6e2052f-542255195", "/tmp/block_0x0bbe9f14-3-0x01b7f3fe-209673214", "/tmp/block_0x0bbe9f14-4-0x0f290422-320999749", "/tmp/block_0x0bbe9f14-5-0x2dc0fb80-844117472", "/tmp/block_0x0bbe9f14-6-0x35542da1-256306111", "/tmp/block_0x0bbe9f14-7-0x3e199a08-086370834", "/tmp/block_0x0bbe9f14-8-0x87778b88-194603593", "/tmp/block_0x0bbe9f14-9-0xbcb081ba-629580052", "/tmp/block_0x0bbe9f14-10-0xc254381a-578605923", "/tmp/block_0x0bbe9f14-11-0xcc434d58-405931366", "/tmp/block_0x0bbe9f14-12-0xce61967d-874423181", "/tmp/block_0x0bbe9f14-13-0x05a20b35-267153288", "/tmp/block_0x0bbe9f14-14-0x4049f61f-606653767", "/tmp/block_0x0bbe9f14-15-0x46d473d2-614457338", "/tmp/block_0x0bbe9f14-16-0x35cf5500-411906321", "/tmp/block_0x0bbe9f14-17-0x79222961-278569788", "/tmp/block_0x0bbe9f14-18-0xad84e7b1-095032683", "/tmp/block_0x0bbe9f14-19-0x4bd48260-019097038", "/tmp/block_0x0bbe9f14-20-0x151741
```

Files are created in a temp-location, with the naming standard `block_blockhash:4>-<txindex>-<txhash:4>-<random suffix>`. Each opcode immediately streams to file, with no in-geth buffering aside from whatever buffering the os normally does.

On the server side, it also adds some more info when regenerating historical state, namely, the reexec-number if `required historical state is not available` is encountered, so a user can experiment with increasing that setting. It also prints out the remaining block until it reaches target:

```
INFO [10-15|13:48:25.263] Regenerating historical state            block=2385959 target=2386012 remaining=53   elapsed=3m30.990537767s
INFO [10-15|13:48:33.342] Regenerating historical state            block=2386012 target=2386012 remaining=0    elapsed=3m39.070073163s
INFO [10-15|13:48:33.343] Historical state regenerated             block=2386012 elapsed=3m39.070454362s nodes=10.03mB preimages=652.08kB
INFO [10-15|13:48:33.352] Wrote trace                              file=/tmp/block_0x14490c57-0-0xfbbd6d91-715824834
INFO [10-15|13:48:33.352] Wrote trace                              file=/tmp/block_0x14490c57-1-0x71076194-187462969
INFO [10-15|13:48:34.421] Wrote trace file=/tmp/block_0x14490c57-2-0x3f4263fe-056924484
```

The `options` is as follows:

```json
type StdTraceConfig struct {
  *vm.LogConfig
  Reexec *uint64
  TxHash *common.Hash
  }
```

```markdown
## `debug_standardTraceBadBlockToFile`

This method is similar to `debug_standardTraceBlockToFile`, but can be used to obtain info about a block which has been rejected as invalid (for some reason).

## `debug_startCPUProfile`

Turns on CPU profiling indefinitely, writing to the given file.

- **Client Method invocation:**
  - **Console:** `debug.startCPUProfile(file)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_startCPUProfile", "params": [string]}
  ```

## `debug_startGoTrace`

Starts writing a Go runtime trace to the given file.

- **Client Method invocation:**
  - **Console:** `debug.startGoTrace(file)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_startGoTrace", "params": [string]}
  ```

## `debug_stopCPUProfile`

Stops an ongoing CPU profile.

- **Client Method invocation:**
  - **Console:** `debug.stopCPUProfile()`
- **RPC Call:**
  ```json
  RPC{"method": "debug_stopCPUProfile", "params": []}
  ```

## `debug_stopGoTrace`

Stops writing the Go runtime trace.

- **Client Method invocation:**
  - **Console:** `debug.stopGoTrace()`
- **RPC Call:**
  ```json
  RPC{"method": "debug_stopGoTrace", "params": []}
  ```

## `debug_storageRangeAt`

Returns the storage at the given block height and transaction index. The result can be paged by providing a `maxResult` to cap the number of storage slots returned as well as specifying the offset via `keyStart` (hash of storage key).

- **Client Method invocation:**
  - **Console:** `debug.storageRangeAt(blockHash, txIdx, contractAddress, keyStart, maxResult)`
- **RPC Call:**
  ```json
  RPC{"method": "debug_storageRangeAt", "params": [blockHash, txIdx, contractAddress, keyStart, maxResult]}
  ```

## `debug_traceBadBlock`

Returns the structured logs created during the execution of EVM against a block pulled from the pool of bad ones and returns them as a JSON object. For the second parameter, see `TraceConfig` reference.

- **Client Method invocation:**
  - **Console:** `debug.traceBadBlock(blockHash, [options])`
- **RPC Call:**
  ```json
  RPC{"method": "debug_traceBadBlock", "params": [blockHash, {}]}
  ```

## `debug_traceBlock`

The `traceBlock` method will return a full stack trace of all invoked opcodes of all transactions that were included in this block. Note, the parent of this block must be present or it will fail. For the second parameter, see `TraceConfig` reference.

- **Client Method invocation:**
  - **Go:** `debug.TraceBlock(blockRlp []byte, config *TraceConfig) BlockTraceResult`
  - **Console:** `debug.traceBlock(tblockRlp, [options])`
- **RPC Call:**
  ```json
  RPC{"method": "debug_traceBlock", "params": [blockRlp, {}]}
  ```
- **References:** [RLP](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/)

**Example:**

```json
> debug.traceBlock("0xblock_rlp")
[
  {
    txHash: "0xabba...",
    result: {
      gas: 85301,
      returnValue: "",
      structLogs: [{
          depth: 1,
          error: "",
          gas: 162106,
          gasCost: 3,
          memory: null,
          op: "PUSH1",
          pc: 0,
          stack: [],
          storage: {}
      },
      /* snip */
      {
          depth: 1,
          error: "",
          gas: 100000,
          gasCost: 0,
          memory: ["0000000000000000000000000000000000000000000000000000000000000006", "0000000000000000000000000000000000000000000000000000000000000000", "0000000000000000000000000000000000000000000000000000000000000060"],
          op: "STOP",
          pc: 120,
          stack: ["00000000000000000000000000000000000000000000000000000000d67cbec9"],
          storage: {
            0000000000000000000000000000000000000000000000000000000000000004: "8241fa522772837f0d05511f20caa6da1d5a3209000000000000000400000001",
            0000000000000000000000000000000000000000000000000000000000000006: "0000000000000000000000000000000000000000000000000000000000000001",
            f652222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f: "00000000000000000000000002e816afc1b5c0f39852131959d946eb3b07b5ad"
          }
      }]
    }
  },
  {
    txHash: "0xacca...",
    result: {
      /* snip */
    }
  }
]
```

## `debug_traceBlockByNumber`

Similar to `debug_traceBlock`, `traceBlockByNumber` accepts a block number and will replay the block that is already present in the database. For the second parameter, see `TraceConfig` reference.

- **Client Method invocation:**
  - **Go:** `debug.TraceBlockByNumber(number uint64, config *TraceConfig) BlockTraceResult`
  - **Console:** `debug.traceBlockByNumber(number, [options])`
- **RPC Call:**
  ```json
  RPC{"method": "debug_traceBlockByNumber", "params": [number, {}]}
  ```
- **References:** [RLP](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/)

## `debug_traceBlockByHash`

Similar to `debug_traceBlock`, `traceBlockByHash` accepts a block hash and will replay the block that is already present in the database. For the second parameter, see `TraceConfig` reference.

- **Client Method invocation:**
  - **Go:** `debug.TraceBlockByHash(hash common.Hash, config *TraceConfig) BlockTraceResult`
  - **Console:** `debug.traceBlockByHash(hash, [options])`
- **RPC Call:**
  ```json
  RPC{"method": "debug_traceBlockByHash", "params": [hash {}]}
  ```
- **References:** [RLP](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/)

## `debug_traceBlockFromFile`

Similar to `debug_traceBlock`, `traceBlockFromFile` accepts a file containing the RLP of the block. For the second parameter, see `TraceConfig` reference.

- **Client Method invocation:**
  - **Go:** `debug.TraceBlockFromFile(fileName string, config *TraceConfig) BlockTraceResult`
  - **Console:** `debug.traceBlockFromFile(fileName, [options])`
- **RPC Call:**
  ```json
  RPC{"method": "debug_traceBlockFromFile", "params": [fileName, {}]}
  ```
- **References:** [RLP](https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/)

## `debug_traceCall`

The `debug_traceCall` method lets you run an `eth_call` within the context of the given block execution using the final state of parent block as the base. The first argument (just as in `eth_call`) is a transaction object. The block can be specified either by hash or by number as the second argument. The trace can be configured similar to `debug_traceTransaction`, see `TraceCallConfig`. The method returns the same output as `debug_traceTransaction`.

```json
{"method": "debug_traceCall", "params": [object, blockNrOrHash, {}]}
```

