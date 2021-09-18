# TxPool v2

Design docs: https://github.com/ledgerwatch/erigon/wiki/Transaction-Pool-Design

Has 2 modes: internal and external

## Internal mode

Works inside Erigon: add `--state.stream --txpool.v2` flags to Erigon, and `--txpool.v2` flag to RPCDaemon

## External mode

Works in separated process and **require** external Sentry. TxPool connect to Erigon and Sentry. RPCDaemon connect to
TxPool. Build by:

```
make txpool
```

Start by:

```
# Add `--state.stream --txpool.disable` flags to Erigon.
./build/bin/sentry
./build/bin/txpool
```

To change address/port of Erigon or Sentry:

```
./build/bin/txpool --private.api.addr localhost:9090 --sentry.api.addr localhost:9091 --txpool.api.addr localhost:9094
```

## Increase pool limits

Will add this part soon [tbd]

## ToDo list

[x] Remote-mode support - with coherent state cache
[x] Persistence
[x] Grafana board
[x] Non-mainnet support
[] DevNet - doesn't send mined block notification on first mined block (because initialCycle = true)
[] Add cli options to manage pool limits
[] Add way for simple introspection - where is tx and why
[] DiscardReasons - user must understand clearly why tx were rejected
[] Hard-forks support (now rules are parsed ones on txPool start)
[] Cache advanced eviction
[] Add pool to docker-compose
[] Add pool (db table) - where store recently mined txs - for faster unwind/reorg.
[] Save history of local transactions - with 1 day expiration
[] Miner - recheck if miner has all EIP-1559 patches
[] Miner - to work on state cache
