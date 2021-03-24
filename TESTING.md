Testing of new releases of Turbo-Geth should ideally include these checks.

## Incremental Sync
This check requires having the turbo-geth database synced previously. Lets assume (for command line examples) it is in the directory `~/mainnet/tg/chaindata`.
Using `git pull` or `git checkout`, update the code to the version that is to be released (or very close to it). Then, build turbo-geth executable:
```
make tg
```
Now run turbo-geth as usually, to try to catch up with the mainnet
```
./build/bin/tg --datadir ~/mainnet
```
It is useful to pipe the output into a text file for later inspection, for example:
```
./build/bin/tg --datadir ~/mainnet 2>&1 | tee tg.log
```

Wait until turbo-geth catches up with the network. This can be determined by looking at the output and seeing that sync cycles usually go through just a single block,
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

After that, it is useful to wait more until an Unwind is encoutered and check that turbo-geth handled it without errors. Usually, errors occur at the stage
`[7/14 IntermediateHashes]` and manifest in the wrong trie root. Here is an example of processing an unwind without errors (look for the word "Unwind" in the log):

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
and also showing how back do we need to rewind to perform the reorg: `forkBlockNumber=12101884`. After that, all the stages are unwound in the "Unwinding order",
which is almost the reverse order of stages, but with some exceptions (like `TxPool` stage). After unwinding all the stages to the `forkBlockNumber`, turbo-geth
applies the new chain branch, in the example above it is two new blocks. In the `Timings` log output one can see the timings of unwinding stages as well as timings
of normal stage operation when the chain branch is applied.

### Errors to ignore for now
There are couple of types of errors that are encountered during this check, which need to be ignored for now, until we handle them better or fix the underlying
issues.
The first error is probably a result of some data race in the code, and this code will be replaced by the new sentry/downloader design, therefore we are not keen
on investigating/fixing it.

```
ERROR[03-24|13:49:53.343] Ethereum peer removal failed             peer=bfa4a38e err="peer not registered"
```

The second error happens during the unwinding the `TxPool` stage. It has been reported in this issue: https://github.com/ledgerwatch/turbo-geth/issues/848
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

this is also likely to disappered after the introduction of new downloader/sentry design

## Fresh sync (only when there are serious changes in the data model)
The way to perform this check is almost the same as the Incremental Sync, but starting with an empty directory for the database. This process takes much longer
(can take 2-3 days on good hardware), that is why it should be done perhaps weekly.

## Executing historical transactions
Having up-to-date database, and having shut down the turbo-geth node (it will work without shutting down, but it will lead to bloating of the database file),
this command can be executed:
```
./build/bin/state checkChangeSets --chaindata ~/mainnet/tg/chaindata --block 11000000
```
Please note the difference in notation when referring to the database. Turbo-geth command uses `--datadir` which points to `~mainnet`, and it looks for the
actual database directory under `tg/chaindata`, but `checkChangeSets` need to be given slightly different path, pointing directly to the database directory.
Parameter `--block` is used to specify from which historical block the execution needs to start.

Normally, this command reports the progress after every 1000 blocks, and if there are no errors after few thousand blocks, this check can be regarded as complete.
We might add another option to this command to specify at which block number to stop, so that this check can be automated.

