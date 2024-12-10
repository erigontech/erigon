Testing of new releases of Erigon should ideally include these checks.

## Incremental Sync

This check requires having the Erigon database synced previously. Let's assume (for command line examples) it is in the
directory `~/mainnet/erigon/chaindata`.
Using `git pull` or `git checkout`, update the code to the version that is to be released (or very close to it). Then,
build erigon executable:

```
make erigon
```

Now run Erigon as usually, to try to catch up with the mainnet

```
./build/bin/erigon --datadir=~/mainnet
```

It is useful to pipe the output into a text file for later inspection, for example:

```
./build/bin/erigon --datadir=~/mainnet 2>&1 | tee erigon.log
```

Wait until Erigon catches up with the network. This can be determined by looking at the output and seeing that sync
cycles usually go through just a single block,
and that block is recent (can be verified on etherscan.io, for example). For example, output may look like this:

```
INFO [03-24|13:41:14.101] [1/14 Headers] Imported new block headers count=1  elapsed=1.621ms     number=12101885 hash="8fe088…b877ee"
INFO [03-24|13:41:14.213] [3/14 Bodies] Downloading block bodies   count=1
INFO [03-24|13:41:17.789] [3/14 Bodies] Imported new chain segment blocks=1 txs=175  elapsed=1.045ms     number=12101885 hash="8fe088…b877ee"
INFO [03-24|13:41:17.793] [4/14 Senders] Started                   from=12101884 to=12101885
INFO [03-24|13:41:17.793] [4/14 Senders] Read canonical hashes     amount=1
INFO [03-24|13:41:17.829] [5/14 Execution] Blocks execution        from=12101884 to=12101885
INFO [03-24|13:41:17.905] [5/14 Execution] Completed on            block=12101885
INFO [03-24|13:41:17.905] [6/14 HashState] Promoting plain state   from=12101884 to=12101885
INFO [03-24|13:41:17.905] [6/14 HashState] Incremental promotion started from=12101884 to=12101885 codes=true  csbucket=PLAIN-ACS
INFO [03-24|13:41:17.912] [6/14 HashState] Incremental promotion started from=12101884 to=12101885 codes=false csbucket=PLAIN-ACS
INFO [03-24|13:41:17.935] [6/14 HashState] Incremental promotion started from=12101884 to=12101885 codes=false csbucket=PLAIN-SCS
INFO [03-24|13:41:17.961] [7/14 IntermediateHashes] Generating intermediate hashes from=12101884 to=12101885
INFO [03-24|13:41:18.064] [7/14 IntermediateHashes] Trie root       hash=0x7937f8c3881e27e1696a36b0a5f84e83792cff7966e97b68059442dadd404368 in=99.621957ms
INFO [03-24|13:41:18.162] [11/14 CallTraces] disabled. Work In Progress
INFO [03-24|13:41:18.225] [13/14 TxPool] Read canonical hashes     hashes=1
INFO [03-24|13:41:18.227] [13/14 TxPool] Transaction stats         pending=3909 queued=1036
INFO [03-24|13:41:18.227] [14/14 Finish] Update current block for the RPC API to=12101885
INFO [03-24|13:41:18.227] Memory                                   alloc=572.23MiB sys=757.02MiB
INFO [03-24|13:41:18.227] Timings                                  Headers=468.891825ms  BlockHashes=1.652309ms   Bodies=3.579430574s    Bodies="619.203µs"     Senders=36.435667ms Execution=75.538895ms  HashState=56.27559ms   IntermediateHashes=113.255714ms AccountHistoryIndex=29.369386ms  StorageHistoryIndex=37.829112ms  LogIndex=20.112169ms  TxLookup=48.53473ms   TxPool=16.63633ms  Finish="37.623µs"
INFO [03-24|13:41:18.227] Tables                                   PLAIN-CST2=10.66GiB PLAIN-SCS=101.86GiB eth_tx=229.75GiB hAT=25.38GiB hST=70.64GiB hashed_accounts=11.74GiB l=81.02GiB log=227.30GiB log_topic_index=18.76GiB r=10.05GiB txSenders=29.52GiB freelist=47.69MiB
INFO [03-24|13:41:20.391] Commit cycle                             in=2.163782297s
```

Here we see that the sync cycle went through all the stages for a single block `12101885`.

After that, it is useful to wait more until an Unwind is encountered and check that Erigon handled it without errors.
Usually, errors occur at the stage
`[7/14 IntermediateHashes]` and manifest in the wrong trie root. Here is an example of processing an unwind without
errors (look for the word "Unwind" in the log):

```
INFO [03-24|13:41:34.318] [1/14 Headers] Imported new block headers count=2  elapsed=1.855ms     number=12101886 hash="20d688…fdaaa6" reorg=true forkBlockNumber=12101884
INFO [03-24|13:41:34.318] UnwindTo                                 block=12101884
INFO [03-24|13:41:34.332] Unwinding...                             stage=TxLookup
INFO [03-24|13:41:34.341] Unwinding...                             stage=LogIndex
INFO [03-24|13:41:34.371] Unwinding...                             stage=StorageHistoryIndex
INFO [03-24|13:41:34.428] Unwinding...                             stage=AccountHistoryIndex
INFO [03-24|13:41:34.462] Unwinding...                             stage=HashState
INFO [03-24|13:41:34.462] [6/14 HashState] Unwinding started       from=12101885 to=12101884 storage=false codes=true
INFO [03-24|13:41:34.464] [6/14 HashState] Unwinding started       from=12101885 to=12101884 storage=false codes=false
INFO [03-24|13:41:34.467] [6/14 HashState] Unwinding started       from=12101885 to=12101884 storage=true  codes=false
INFO [03-24|13:41:34.472] Unwinding...                             stage=IntermediateHashes
INFO [03-24|13:41:34.472] [7/14 IntermediateHashes] Unwinding of trie hashes from=12101885 to=12101884 csbucket=PLAIN-ACS
INFO [03-24|13:41:34.474] [7/14 IntermediateHashes] Unwinding of trie hashes from=12101885 to=12101884 csbucket=PLAIN-SCS
INFO [03-24|13:41:34.511] [7/14 IntermediateHashes] Trie root      hash=0xa06f932426150e3a8c78607fc873bb15259c57fc2a1b8136ed4065073b9ee6b6 in=33.835208ms
INFO [03-24|13:41:34.518] Unwinding...                             stage=Execution
INFO [03-24|13:41:34.518] [5/14 Execution] Unwind Execution        from=12101885 to=12101884
INFO [03-24|13:41:34.522] Unwinding...                             stage=Senders
INFO [03-24|13:41:34.522] Unwinding...                             stage=TxPool
INFO [03-24|13:41:34.537] [13/14 TxPool] Read canonical hashes     hashes=1
INFO [03-24|13:41:34.537] [13/14 TxPool] Injecting txs into the pool number=0
INFO [03-24|13:41:34.537] [13/14 TxPool] Injection complete
INFO [03-24|13:41:34.538] [13/14 TxPool] Transaction stats         pending=4090 queued=1023
INFO [03-24|13:41:34.538] Commit cycle
INFO [03-24|13:41:36.643] Unwinding...                             stage=Bodies
INFO [03-24|13:41:36.645] Unwinding...                             stage=BlockHashes
INFO [03-24|13:41:36.647] Unwinding...                             stage=Headers
INFO [03-24|13:41:36.727] [3/14 Bodies] Downloading block bodies   count=2
INFO [03-24|13:41:36.769] [3/14 Bodies] Downloader queue stats     receiptTasks=0 blockTasks=0 itemSize=45.70KiB throttle=1434
INFO [03-24|13:41:36.772] [3/14 Bodies] Imported new chain segment blocks=2 txs=408  elapsed=2.032ms     number=12101886 hash="20d688…fdaaa6"
INFO [03-24|13:41:36.776] [4/14 Senders] Started                   from=12101884 to=12101886
INFO [03-24|13:41:36.776] [4/14 Senders] Read canonical hashes     amount=2
INFO [03-24|13:41:36.809] [5/14 Execution] Blocks execution        from=12101884 to=12101886
INFO [03-24|13:41:36.911] [5/14 Execution] Completed on            block=12101886
INFO [03-24|13:41:36.912] [6/14 HashState] Promoting plain state   from=12101884 to=12101886
INFO [03-24|13:41:36.912] [6/14 HashState] Incremental promotion started from=12101884 to=12101886 codes=true  csbucket=PLAIN-ACS
INFO [03-24|13:41:36.917] [6/14 HashState] Incremental promotion started from=12101884 to=12101886 codes=false csbucket=PLAIN-ACS
INFO [03-24|13:41:36.946] [6/14 HashState] Incremental promotion started from=12101884 to=12101886 codes=false csbucket=PLAIN-SCS
INFO [03-24|13:41:36.972] [7/14 IntermediateHashes] Generating intermediate hashes from=12101884 to=12101886
INFO [03-24|13:41:37.100] [7/14 IntermediateHashes] Trie root       hash=0x38aa726427894df05393eb41779f3f74e69dd68553ad301aaf9a9fdb2e60d53d in=120.544291ms
INFO [03-24|13:41:37.228] [11/14 CallTraces] disabled. Work In Progress
ERROR[03-24|13:41:37.323] Demoting invalidated transaction         hash="7f686f…05d457"
ERROR[03-24|13:41:37.343] Demoting invalidated transaction         hash="e809f5…81245e"
INFO [03-24|13:41:37.347] [13/14 TxPool] Read canonical hashes     hashes=2
INFO [03-24|13:41:37.353] [13/14 TxPool] Transaction stats         pending=3885 queued=1039
INFO [03-24|13:41:37.353] [14/14 Finish] Update current block for the RPC API to=12101886
INFO [03-24|13:41:37.353] Memory                                   alloc=314.65MiB sys=757.02MiB
INFO [03-24|13:41:37.353] Timings (first 50)                       Headers=72.394932ms   Unwind TxLookup=9.141155ms  Unwind LogIndex=29.275311ms Unwind StorageHistoryIndex=57.562267ms Unwind AccountHistoryIndex=33.71906ms  Unwind HashState=10.102642ms Unwind IntermediateHashes=46.475857ms Unwind Execution=3.449326ms Unwind Senders="
31.255µs" Unwind TxPool=16.378952ms Unwind Bodies=1.81026ms  Unwind BlockHashes=1.636441ms Unwind Headers=1.883299ms Headers=77.905179ms   BlockHashes="138.005µs"  Bodies=48.816311ms     Bodies="551.032µs"     Senders=33.397734ms Execution=102.095383ms HashState=60.080073ms  IntermediateHashes=142.317001ms AccountHistoryIndex=45.261
032ms  StorageHistoryIndex=42.51298ms   LogIndex=26.347729ms  TxLookup=78.822422ms
INFO [03-24|13:41:37.353] Tables                                   PLAIN-CST2=10.66GiB PLAIN-SCS=101.86GiB eth_tx=229.75GiB hAT=25.38GiB hST=70.64GiB hashed_accounts=11.74GiB l=81.02GiB log=227.30GiB log_topic_index=18.76GiB r=10.05GiB txSenders=29.52GiB freelist=47.69MiB
INFO [03-24|13:41:39.443] Commit cycle                             in=2.090509095s
```

In this example, the Unwind starts with the stage `[1/14 Headers]` reporting the reorg: `reorg=true`
and also showing how back do we need to rewind to perform the reorg: `forkBlockNumber=12101884`. After that, all the
stages are unwound in the "Unwinding order",
which is almost the reverse order of stages, but with some exceptions (like `TxPool` stage). After unwinding all the
stages to the `forkBlockNumber`, Erigon
applies the new chain branch, in the example above it is two new blocks. In the `Timings` log output one can see the
timings of unwinding stages as well as timings
of normal stage operation when the chain branch is applied.

### Errors to ignore for now

There are couple of types of errors that are encountered during this check, which need to be ignored for now, until we
handle them better or fix the underlying
issues.
The first error is probably a result of some data race in the code, and this code will be replaced by the new
sentry/downloader design, therefore we are not keen
on investigating/fixing it.

```
ERROR[03-24|13:49:53.343] Ethereum peer removal failed             peer=bfa4a38e err="peer not registered"
```

The second error happens during the unwinding the `TxPool` stage. It has been reported in this
issue: https://github.com/erigontech/erigon/issues/848

```
ERROR[08-01|14:30:38.297] Demoting invalidated transaction         hash="6ee8a8…92bf22"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="7abab0…eccbed"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="de485c…dcd575"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="3d33a1…bc8f61"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="b11d49…481e7c"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="832d13…076faa"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="859191…0366c0"
ERROR[08-01|14:30:38.299] Demoting invalidated transaction         hash="25ee67…e73153"
```

this is also likely to disappear after the introduction of new downloader/sentry design

### Assessing relative performance of sync

From one release to another, there should not be noticeable degradation in the sync performance, unless there are clear
reasons for it. These reasons
may include raised gas limit on Ethereum network, adding new table to the database, etc.

In order to assess relative performance of the sync compared to the previous release, one needs to capture these log
lines from the log files
(but only after the sync has "stabilised", meaning it is over few initial long sync cycles:

```
INFO [03-24|13:41:18.227] Timings                                  Headers=468.891825ms  BlockHashes=1.652309ms   Bodies=3.579430574s    Bodies="619.203µs"     Senders=36.435667ms Execution=75.538895ms  HashState=56.27559ms   IntermediateHashes=113.255714ms AccountHistoryIndex=29.369386ms  StorageHistoryIndex=37.829112ms  LogIndex=20.112169ms  TxLookup=48.53473ms   TxPool=16.63633ms  Finish="37.623µs"
```

The line above shows how much time each stage of processing takes.

```
INFO [03-24|13:41:20.391] Commit cycle                             in=2.163782297s
```

The line above shows how long was commit cycle. We saw in the past that after some changes the commit time dramatically
increases, and these
regressions need to be investigated. We expect "commit cycle" on Linux with NVMe drive to usually take less than a
second. For other operating
systems and devices the typical time may vary, but it should significantly increase from one release to another.
Perhaps we need to log some extra information in the log to make it easier for the tester to filter out the log
statements after the sync "stabilised".

## Fresh sync (only when there are serious changes in the data model)

The way to perform this check is almost the same as the Incremental Sync, but starting with an empty directory for the
database. This process takes much longer
(can take 2-3 days on good hardware), that is why it should be done perhaps weekly.

## Executing historical transactions

Having up-to-date database, and having shut down the Erigon node (it will work without shutting down, but it will lead
to bloating of the database file),
this command can be executed:

```
./build/bin/state checkChangeSets --datadir=<path to datadir> --block=1_1000_000
```

Please note the difference in notation when referring to the database. Erigon command uses `--datadir` which points
to `~mainnet`, and it looks for the
actual database directory under `erigon/chaindata`, but `checkChangeSets` need to be given slightly different path,
pointing directly to the database directory.
Parameter `--block` is used to specify from which historical block the execution needs to start.

Normally, this command reports the progress after every 1000 blocks, and if there are no errors after few thousand
blocks, this check can be regarded as complete.
We might add another option to this command to specify at which block number to stop, so that this check can be
automated.

## JSON RPC comparative tests

These tests have been originally created to compare the responses that RPC daemon gives to various JSON RPC queries with
go-ethereum and OpenEthereum.
However, such tests require access to archive nodes of go-ethereum or OpenEthereum, and those require a long time to set
up. Therefore, if we make
assumption that the previous release of Erigon worked correctly, we can compare behaviour of the new release compared to
the previous one.
To set this up, imagine that we have two computer, on the network addresses `192.168.1.1` (previous release)
and `192.168.1.2` (new release).
On the first computer, we launch Erigon:

```
git checkout PREV_RELEASE_TAG
git pull
make erigon
./build/bin/erigon --datadir=~/mainnet --private.api.addr=192.168.1.1:9090
```

And in another terminal window (or in other way to launch separate process), RPC daemon connected to it (it can also be
launched on a different computer)

```
git checkout PREV_RELEASE_TAG
git pull
make rpcdaemon
./build/bin/rpcdaemon --private.api.addr=192.168.1.1:9090 --http.addr=192.168.1.1 --http.port=8545 --http.api=eth,debug,trace
```

Note that if Erigon and RPC daemon are running on the same computer, they can also communicate via loopback (`localhost`
, `127.0.0.1`) interface. To
make this happen, pass `--private.api.addr=localhost:9090` or `--private.api.addr=127.0.0.1:9090` to both Erigon and RPC
daemon. Also note that
choice of the port `9090` is arbitrary, it can be any port free port number, as long as the value matches in Erigon and
RPC daemon.

On the second computer (or on the same computer, but using different directories and port numbers), the same combination
of processes is launched,
but from the `master` branch or the tag that is being tested:

```
git checkout master
git pull
make erigon
./build/bin/erigon --datadir=~/mainnet --private.api.addr=192.168.1.2:9090
```

```
git checkout master
git pull
make rpcdaemon
./build/bin/rpcdaemon --private.api.addr=192.168.1.2:9090 --http.addr=192.168.1.2 --http.port=8545 --http.api=eth,debug,trace
```

Once both RPC daemons are running, RPC test utility can be run:

```
git checkout master
git pull
make rpctest
./build/bin/rpctest bench8 --erigonUrl http://192.168.1.2:8545 --gethUrl http://192.168.1.1:8545 --needCompare --blockFrom 9000000 --blockTo 9000100
```

In the example above, the `bench8` command is used. RPC test utility has a few of such "benches". These benches
automatically generate JSON RPC
requests for certain RPC methods, using hits provided by options `--blockFrom` and `--blockTo`. Currently the most
useful ones are:

1. `bench8` tests `eth_getLogs` RPC method (compatibility with go-ethereum and OpenEthereum)
2. `bench11` tests `trace_call` RPC method (compatibility with OpenEthereum tracing)
3. `bench12` tests `debug_traceCall` RPC method (compatibility with go-ethereum tracing)
4. `bench13` tests `trace_callMany` RPC method (compatibility with OpenEthereum tracing)

Options `--erigonUrl` and `--gethUrl` specify HTTP endpoints that need to be tested against each other. Despite its
name, `--gethUrl` option does not have to
point to go-ethereum node, it can point to anything that it supposed to be "correct" for the purpose of the test (
go-ethereum node, OpenEthereum node,
or Erigon RPC daemon & Erigon node built from the previous release code).

Option `--needCompare` triggers the comparison of JSON RPC responses. If omitted, requests to `--gethUrl` are not done.
When comparison is turned on,
the utility stops at the first occurrence of mismatch.

## RPC test recording and replay

In order to facilitate the automation of testing, we are adding the ability to record JSON RPC requests generated by RPC
test utility, and responses
received from Erigon. Once these are recorded in a file, they can be later replayed, without the need of having the
second RPC daemon present.
To turn on recording, option `--recordFile <filename>` needs to be added. Currently, only `bench8`, `bench11`
and `bench13` support recording.
Only queries and responses for which the comparison produced the match, are recorded. If `--needCompare` is not
specified, but `--recordFile` is,
then all generated queries and responses are recorded. This can be used to separate the testing into 2 parts in time.

Example of recording command:

```
./build/bin/rpctest bench8 --erigonUrl http://192.168.1.2:8545 --gethUrl http://192.168.1.1:8545 --needCompare --blockFrom 9000000 --blockTo 9000100 --recordFile req.txt
```

The file format is plain text, with requests and responses are written in separate lines, and delimited by the triple
line breaks, like this:

```
{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x7a1200", "toBlock": "0x7a3974", "address": "0xcf8800f7cd90cbf3b7f5eb3681db88d8c686f54b", "topics": ["0x0000000000000000000000000000000000000000000000000000000000000000"]}],"id":63}
{"jsonrpc":"2.0","id":63,"result":[]}


{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x7a1200", "toBlock": "0x7a1264", "address": "0xe522dc278cf97ce026e461dca63331dc64274dec"}],"id":64}
{"jsonrpc":"2.0","id":64,"result":[{"address":"0xe522dc278cf97ce026e461dca63331dc64274dec","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000005663028af4745308d5249de455e096f6eac12635","0x000000000000000000000000c41e18a3de27615492b8a6927ddf2735f12ed88f"],"data":"0x000000000000000000000000000000000000000000000001403ade3e50238000","blockNumber":"0x7a1236","transactionHash":"0xc722e8133ade4d29c1221ac211ffc886d416250f0910ebfa3dd6c72f006303c2","transactionIndex":"0x60","blockHash":"0xfd36ee4392c693c4b9f8cacef0816225077b3dd565e68945ab5a248fc8fb8b29","logIndex":"0x49","removed":false},{"address":"0xe522dc278cf97ce026e461dca63331dc64274dec","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000005663028af4745308d5249de455e096f6eac12635","0x000000000000000000000000fc10cab6a50a1ab10c56983c80cc82afc6559cf1"],"data":"0x0000000000000000000000000000000000000000000000000000000000000000","blockNumber":"0x7a1236","transactionHash":"0xc722e8133ade4d29c1221ac211ffc886d416250f0910ebfa3dd6c72f006303c2","transactionIndex":"0x60","blockHash":"0xfd36ee4392c693c4b9f8cacef0816225077b3dd565e68945ab5a248fc8fb8b29","logIndex":"0x4a","removed":false}]}


{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x7a1200", "toBlock": "0x7a3974", "address": "0xe522dc278cf97ce026e461dca63331dc64274dec", "topics": ["0x0000000000000000000000000000000000000000000000000000000000000000"]}],"id":65}
{"jsonrpc":"2.0","id":65,"result":[]}


```

To replay recorded queries, `replay` command can be used:

```
./build/bin/rpctest replay --erigonUrl http://192.168.1.2:8545 --recordFile req.txt
```

