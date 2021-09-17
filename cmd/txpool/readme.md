# TxPool

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
