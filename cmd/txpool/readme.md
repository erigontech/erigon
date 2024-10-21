# TxPool v2

Transaction Pool - place where living "not-included-to-block-yet transactions".
Erigon's TxPool can work inside Erigon (default) and as separated process.

Erigon's pool implementation is not fork of Geth’s - Design
docs: https://github.com/erigontech/erigon/wiki/Transaction-Pool-Design
95% of pool-related code (from p2p message parsing, to sorting logic) is inside this
folder: https://github.com/erigontech/erigon/tree/main/cmd/txpool

## Internal mode

It's default. No special flags required - just start Erigon.
RPCDaemon - flags `--private.api.addr` and `--txpool.api.addr` must have same value in this case.

## External mode

```
make txpool

# Add `--txpool.disable` flag to Erigon

# External TxPool require(!) external Sentry
./build/bin/sentry --sentry.api.addr=localhost:9091 --datadir=<your_datadir>

# Start TxPool service (it connects to Erigon and Sentry):
# --private.api.addr - connect to Erigon's grpc api
# --sentry.api.addr  - connect to Sentry's grpc api
# --txpool.api.addr  - other services to connect TxPool's grpc api
# Increase limits flags: --txpool.globalslots, --txpool.globalbasefeeslots, --txpool.globalqueue
# --txpool.trace.senders - print more logs about Txs with senders in this list
./build/bin/txpool --private.api.addr=localhost:9090 --sentry.api.addr=localhost:9091 --txpool.api.addr=localhost:9094 --datadir=<your_datadir>

# Add flag `--txpool.api.addr` to RPCDaemon
```

## ToDo list

[] Hard-forks support (now TxPool require restart - after hard-fork happens)
[] Add pool to docker-compose
[] Add pool (db table) - where store recently mined txs - for faster unwind/reorg.
[] Save history of local transactions - with 1 day expiration
[] move tx.rlp field to separated map, to make txn immutable
