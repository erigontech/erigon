Integration - tool to run Erigon stages in custom way: run/reset single stage, run all stages but reorg every X blocks,
etc...

## Examples

All commands require parameter `--datadir=<datadir>` - I will skip it for readability.

```
integration --help
integration print_stages

# Run single stage
integration stage_senders
integration stage_exec
integration stage_exec --block=1_000_000 # stop at 1M block
integration stage_exec --sync.mode.chaintip # every block: `ComputeCommitment`, `rwtx.Commit()`, write diffs/changesets
integration stage_hash_state
integration stage_trie
integration stage_history
integration stage_tx_lookup

# Unwind single stage 10 blocks backward
integration stage_exec --unwind=10

# Drop data of single stage
integration stage_exec --reset
integration stage_history --reset

# Unwind single stage N blocks backward
integration stage_exec --unwind=N
integration stage_history --unwind=N

# Run stage prune to block N
integration stage_exec --prune.to=N
integration stage_history --prune.to=N

# Reset stage_headers
integration stage_headers --reset --datadir=<my_datadir> --chain=<my_chain>

# Exec blocks, but don't commit changes (loose them)
integration stage_exec --no-commit
...

# Run txn replay with domains [requires 6th stage to be done before run]
integration state_domains --chain sepolia --last-step=4 # stop replay when 4th step is merged
integration read_domains --chain sepolia account <addr> <addr> ... # read values for given accounts

# hack which allows to force clear unwind stack of all stages
clear_unwind_stack
```

## For testing run all stages in "N blocks forward M blocks re-org" loop

Pre-requirements of `state_stages` command:

- Headers/Bodies must be downloaded
- TxSenders stage must be executed

```
make all
./build/bin/integration state_stages --datadir=<datadir> --unwind=10 --unwind.every=20 --pprof
integration reset_state # drops all stages after Senders stage (including it's db tables DB tables)
```

For example:

```
--unwind=1 --unwind.every=10  # 10 blocks forward, 1 block back, 10 blocks forward, ...
--unwind=10 --unwind.every=1  # 1 block forward, 10 blocks back, 1 blocks forward, ...
--unwind=10  # 10 blocks back, then stop
--integrity.fast=false --integrity.slow=false # it performs DB integrity checks each step. You can disable slow or fast checks.
--block # stop at exact blocks
--chaindata.reference # When finish all cycles, does comparison to this db file.
```

## "Wrong trie root" problem - temporary solution

```
make all
./build/bin/integration stage_hash_state --datadir=<datadir> --reset
./build/bin/integration stage_trie --datadir=<datadir> --reset
# Then run TurobGeth as usually. It will take 2-3 hours to re-calculate dropped db tables
```

## Copy data to another db

```
0. You will need 2x disk space (can be different disks).
1. Stop Erigon
2. Create new db with new --db.pagesize:
ONLY_CREATE_DB=true ./build/bin/erigon --datadir=/erigon-new/ --chain="$CHAIN" --db.pagesize=8kb --db.size.limit=12T
# if erigon doesn't stop after 1 min. just stop it.
3. Build integration: cd erigon; make integration
5. Run: ./build/bin/integration mdbx_to_mdbx --chaindata /existing/erigon/path/chaindata/ --chaindata.to /erigon-new/chaindata/
6. cp -R /existing/erigon/path/snapshots /erigon-new/snapshots
7. start erigon in new datadir as usually
```

## Recover db from some bad state

If you face db-open error like `MDBX_PROBLEM: Unexpected internal error`. First: use tools
like https://www.memtest86.com to test RAM and tools like https://www.smartmontools.org to test Disk. If hardware is
fine: can try manually recover db (see `./build/bin/mdbx_chk -h` for more details):

```
make db-tools

./build/bin/mdbx_chk -0 -d /erigon/chaindata
./build/bin/mdbx_chk -1 -d /erigon/chaindata
./build/bin/mdbx_chk -2 -d /erigon/chaindata

# if all 3 commands return success - then remove `-d` parameter and run again
# if all 1 command is fail but other success. choose successful number - for example 2 - and switch db manually to it:  
./build/bin/mdbx_chk -2 -d -t -w /erigon/chaindata  

# if all 3 commands are fail - game over. use backups.
```

## Clear bad blocks markers table in the case some block was marked as invalid after some error

It allows to process this blocks again

```
1. ./build/bin/integration clear_bad_blocks --datadir=<datadir>
```
