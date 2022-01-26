# TxPool v2

Transaction Pool - place where living "not-included-to-block-yet transactions".
Erigon's TxPool can work inside Erigon (default) and as separated process.

Erigon's pool implementation is not fork of Geth’s, has Apache license - Design
docs: https://github.com/ledgerwatch/erigon/wiki/Transaction-Pool-Design
95% of pool-related code (from p2p message parsing, to sorting logic) is inside this
folder: https://github.com/ledgerwatch/erigon-lib/tree/main/txpool

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
# Add `--txpool.disable` flag to Erigon

# External TxPool require external Sentry
./build/bin/sentry --sentry.api.addr=localhost:9091 --datadir=<your_datadir>

# Start pool service:
# --private.api.addr - connect to Erigon's grpc api
# --sentry.api.addr  - connect to Sentry's grpc api
# --txpool.api.addr  - other services to connect TxPool's grpc api
./build/bin/txpool --private.api.addr=localhost:9090 --sentry.api.addr=localhost:9091 --txpool.api.addr=localhost:9094 --datadir=<your_datadir>

# Increase pool limits by flags: --txpool.globalslots, --txpool.globalbasefeeeslots, --txpool.globalqueue 
```

## ToDo list

[] Hard-forks support (now TxPool require restart - after hard-fork happens)
[] Add pool to docker-compose
[] Add pool (db table) - where store recently mined txs - for faster unwind/reorg.
[] Save history of local transactions - with 1 day expiration
[] move tx.rlp field to separated map, to make tx immutable

