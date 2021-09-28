# TxPool v2

Design docs: https://github.com/ledgerwatch/erigon/wiki/Transaction-Pool-Design

Has 2 modes: internal and external

## Internal mode

Works inside Erigon: add `--txpool.v2` flags to Erigon, and `--txpool.v2` flag to RPCDaemon

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
[x] DevNet - doesn't send mined block notification on first mined block (because initialCycle = true)
[x] DiscardReasons - user must understand clearly why tx were rejected
[x] Notify about new pending transactions - we sending more than need
[] Add cli options to manage pool limits: --txpool.pricelimit, --txpool.globalslots, --txpool.globalqueue (now each
sub-pool has limit 200_000, and no pricelimit)
[] Add way for simple introspection - where is tx and why
[] Hard-forks support (now rules are parsed ones on txPool start)
[] Add pool to docker-compose
[] Add pool (db table) - where store recently mined txs - for faster unwind/reorg.
[] Save history of local transactions - with 1 day expiration
