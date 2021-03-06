Integration - tool to run TurboGeth stages in custom way: run/reset single stage, run all stages but reorg every X blocks, etc...

## Examples 
All commands require parameter `--chaindata=<datadir>/tg/chaindata` - I will skip it for readability.

```
integration --help
integration print_stages

# Run single stage 
integration stage_senders 
integration stage_exec  
integration stage_exec --block=1_000_000 # stop at 1M block
integration stage_hash_state 
integration stage_trie
integration stage_history
integration stage_tx_lookup

# Unwind single stage 10 blocks backward
integration stage_exec --unwind=10

# Drop data of single stage 
integration stage_exec --reset     
integration stage_history --reset
... 

# hack which allows to force clear unwind stack of all stages
clear_unwind_stack
```

## For testing run all stages in "N blocks forward M blocks re-org" loop

Pre-requirements of `state_stages` command:
- Headers/Bodies must be downloaded
- TxSenders stage must be executed

```
make all
./build/bin/integration state_stages --chaindata=<datadir>/tg/chaindata --unwind=10 --unwind.every=20 --pprof
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
./build/bin/integration stage_hash_state --chaindata=<datadir>/tg/chaindata --reset
./build/bin/integration stage_trie --chaindata=<datadir>/tg/chaindata --reset
# Then run TurobGeth as usually. It will take 2-3 hours to re-calculate dropped db tables
```