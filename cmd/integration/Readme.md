Integration - tool to run Erigon stages in custom way: run/reset single stage, run all stages but reorg every X blocks,
etc...

## Examples

All commands require parameter `--datadir=<datadir>` - I will skip it for readability.

```sh
integration --help
integration print_stages

# Run single stage
integration stage_senders
integration stage_exec
integration stage_exec --block=1_000_000 # stop at 1M block
integration stage_exec --sync.mode.chaintip # every block: `ComputeCommitment`, `rwtx.Commit()`, write diffs/changesets

# Unwind single stage 10 blocks backward
integration stage_exec --unwind=10

# Drop data of single stage
integration stage_exec --reset

# Unwind single stage N blocks backward
integration stage_exec --unwind=N

# Run stage prune to block N
integration stage_exec --prune.to=N

# To remove all blocks (together with bodies/txs) from db 
integration stage_headers --reset --datadir=<my_datadir> --chain=<my_chain>

# Exec blocks, but don't commit changes (loose them)
integration stage_exec --no-commit

# Run txn replay with domains [requires 6th stage to be done before run]
integration read_domains --chain sepolia account <addr> <addr> ... # read values for given accounts
```

## For testing run all stages in "N blocks forward M blocks re-org" loop

Pre-requirements of `state_stages` command:

- Headers/Bodies must be downloaded
- TxSenders stage must be executed

```sh
make all
./build/bin/integration state_stages --datadir=<datadir> --unwind=10 --unwind.every=20 --pprof
integration reset_state # drops all stages after Senders stage (including it's db tables DB tables)
```

For example:

```sh
--unwind=1 --unwind.every=10  # 10 blocks forward, 1 block back, 10 blocks forward, ...
--unwind=10 --unwind.every=1  # 1 block forward, 10 blocks back, 1 blocks forward, ...
--unwind=10  # 10 blocks back, then stop
--integrity.fast=false --integrity.slow=false # it performs DB integrity checks each step. You can disable slow or fast checks.
--block # stop at exact blocks
--chaindata.reference # When finish all cycles, does comparison to this db file.
```

## How to unwind node

In Erigon3 - better do `rm -rf chaindata` (for bor maybe also need remove `polygon-bridge`, `bor`, `heimdall` folders
until https://github.com/erigontech/erigon/issues/13674 is fixed)

## Copy data to another db

In Erigon3 - better do `rm -rf chaindata` (for bor maybe also need remove `polygon-bridge`, `bor`, `heimdall` folders
until https://github.com/erigontech/erigon/issues/13674 is fixed)

```sh
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

```sh
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

# FAQ

## How to re-exec all blocks

```sh
# Option 1 (on empty datadir):
erigon --snap.skip-state-snapshot-download

# Option 2 (on synced datadir):
erigon snapshots rm-all-state-state
integration stage_exec --reset
integration stage_exec 

# Option 2 is good 
```

## How to re-gen CommitmentDomain

```sh
integration commitment_rebuild
```

## How to re-generate optional Domain/Index

```sh
# By parallel executing blocks on existing historical state. Can be 1 or many domains:
erigon snapshots rm-state-snapshots --domain=receipt,rcache,logtopics,logaddrs,tracesfrom,tracesto
integration stage_custom_trace --domain=receipt,rcache,logtopics,logaddrs,tracesfrom,tracesto --reset
integration stage_custom_trace --domain=receipt,rcache,logtopics,logaddrs,tracesfrom,tracesto
```

## How to re-gen bor checkpoints

```sh
rm -rf datadir/heimdall
rm -rf datadir/snapshots/*borch*
# Start erigon, it will gen. Then:
erigon snapshots integrity --datadir /erigon-data/ --check=BorCheckpoints
```

## See tables size

```sh
./build/bin/mdbx_stat -efa /erigon-data/chaindata/   | awk '
    BEGIN { pagesize = 4096 }
    /^  Pagesize:/ { pagesize = $2 }
    /^Status of/ { table = $3 }
    /Branch pages:/ { branch = $3 }
    /Leaf pages:/ { leaf = $3 }
    /Overflow pages:/ { overflow = $3 }
    /Entries:/ {
      total_pages = branch + leaf + overflow
      size_gb = (total_pages * pagesize) / (1024^3)
      printf "%-30s %.3fG\n", table, size_gb
    }
  ' | grep -v '0.000G'
```
