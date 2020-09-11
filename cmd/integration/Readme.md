Integration - tool to run TurboGeth stages in custom way: run single stage, or run all stages but reorg every X blocks, etc...

By examples. 
All commands require parameter `--chaindata=/path/to/chaindata` - I will skip it for readability.

```
integration --help
integration print_stages --block=1_000_000

# run all stages (which do not need internet). 
integration state_stages --unwind=1 --unwind_every=10  # process 10 blocks, then reorg 1 block, then process 10 blocks, ... 
integration state_stages --unwind=10 --unwind_every=1  # process 1 block, then reorg 10 blocks, then process 1 blocks, ...

# Run single stage 
integration stage_senders 
integration stage_exec  
integration stage_exec --block=1_000_000 # stop at 1M block
integration stage_hash_state 
integration stage_ih 
integration stage_history
integration stage_tx_lookup

# Drop data of single stage 
integration stage_exec --reset     
integration stage_history --reset
... 

# reset all data after stage_senders
integration reset_state

# hack which allows to force clear unwind stack of all stages
clear_unwind_stack
```

The way I usually run it: 
```
go run -trimpath ./cmd/integration state_stages --chaindata=/path/to/chaindata --unwind=10 --unwind_every=20 --pprof 
```

Pre-requirements of `state_stages` command:
- Headers/Bodies must be downloaded 
- TxSenders stage must be executed