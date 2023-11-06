# Single Process

How to run Erigon in a single process (all parts of the system run as one).

## Commands

1. Build erigon: `make erigon`
2. Run erigon:
    ``` 
    ./build/bin/erigon --datadir=/desired/path/to/datadir \
     --chain=goerli" \
     --port=30304" \
     --http.port=8546 \
     --authrpc.port=8552 \
     --torrent.port=42068 \
     --private.api.addr=127.0.0.1:9091 \
     --http \
     --ws \
     --http.api=eth,debug,net,trace,web3,erigon \
     --log.dir.path=/desired/path/to/logs
     --log.dir.prefix=filename
     ```

## Notes

This runs Erigon with RPCDaemon, TxPool etc. all in one single process. This is the 'simplest' way to run Erigon, reasons for splitting parts out may be that you wish to put different parts of the system on different machines due to performance requirements/load.

## Flags of Interest

- `--chain` dictates the chain (goerli/mainnet etc.) - https://chainlist.org/ is a helpful resource
- `--log.dir.path` dictates where logs will be output - useful for sending reports to the Erigon team when issues occur
- `--http.api` defines the set of APIs which are enabled, the above example is a pretty comprehensive list - what these do is beyond the scope of this example
- `--authrpc.port` is the port which the consensus layer (PoS) uses to talk to Erigon
