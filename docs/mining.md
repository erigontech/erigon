## Mining

**Disclaimer: this document is out of date!**

Support only remote-miners.

* To enable, add `--mine --miner.etherbase=...` or `--mine --miner.sigfile=...` flags.
* Other supported options: `--miner.extradata`, `--miner.notify`, `--miner.gaslimit`, `--miner.gasprice`
  , `--miner.gastarget`
* RPCDaemon supports methods: eth_coinbase , eth_hashrate, eth_mining, eth_getWork, eth_submitWork, eth_submitHashrate
* RPCDaemon supports websocket methods: newPendingTransaction

## Implementation details

* mining implemented as independent 🔬[Staged Sync](/eth/stagedsync/)
* stages are declared in `eth/stagedsync/stagebuilder.go:MiningStages`
* mining work done inside 1 db transaction which RollingBack after block prepared and `--miner.notify` notifications
  sent

## Testing

Integration tool - supports mining of existing blocks. It moves Erigon to block X, does mine block X+1, then compare
mined block with real block X+1 in db. To enable - just add `--mine --miner.etherbase=<etherbase>` flag
to `integration state_stages` command:

```
./build/bin/integration state_stages --datadir=<datadir> --unwind=1 --unwind.every=2 --integrity.fast=false --integrity.slow=false --mine --miner.etherbase=<etherbase>
```
