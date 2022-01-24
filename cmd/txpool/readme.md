# TxPool v2

it’s exactly what you run inside Erigon now, but you also can disable pool inside erigon by —txpool.disable and run it outside ( so can run multiple of them, or use your own implementation, or customize logic).

Same thing you can do with sentry. Also, after the-megre you will run external consensus service.

95% of pool-related code (from p2p message parsing, to sorting logic) is inside this folder: https://github.com/ledgerwatch/erigon-lib/tree/main/txpool

Our pool implementation is not fork of Geth’s ( And it’s Apache licensed) - Design docs: https://github.com/ledgerwatch/erigon/wiki/Transaction-Pool-Design

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

