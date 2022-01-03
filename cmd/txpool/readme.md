# TxPool v2

Design docs: https://github.com/ledgerwatch/erigon/wiki/Transaction-Pool-Design

Has 2 modes: internal and external

## Internal mode

It's default. No special flags required. But if in RPCDaemon you using custom `--private.api.addr` flag, then set same
value to `--txpool.api.addr` flag.

## External mode

Add `--txpool.disable` to erigon. External TxPool works in separated process and **require** external Sentry. TxPool
connect to Erigon and Sentry. RPCDaemon connect to TxPool. Build by:

```
make txpool
```

Start by:

```
# Add `--txpool.disable` flags to Erigon.
./build/bin/sentry
./build/bin/txpool
```

To change address/port of Erigon or Sentry:

```
./build/bin/txpool --private.api.addr localhost:9090 --sentry.api.addr localhost:9091 --txpool.api.addr localhost:9094
```

## Increase pool limits

In `./build/bin/txpool --help` see flags: `--txpool.globalslots`, `--txpool.globalbasefeeeslots`, `--txpool.globalqueue`

## ToDo list

[] Hard-forks support (now TxPool require restart - after hard-fork happens)
[] Add pool to docker-compose
[] Add pool (db table) - where store recently mined txs - for faster unwind/reorg.
[] Save history of local transactions - with 1 day expiration
[] move tx.rlp field to separated map, to make tx immutable

