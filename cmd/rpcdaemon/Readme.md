In turbo-geth RPC calls are extracted out of the main binary into a separate daemon.
This daemon can use both local or remote DBs. That means, that this RPC daemon
doesn't have to be running on the same machine as the main turbo-geth binary or
it can run from a snapshot of a database for read-only calls. [Docs](./cmd/rpcdaemon/Readme.md)

### Get started
**For local DB**

```
> make rpcdaemon
> ./build/bin/rpcdaemon --chaindata ~/Library/TurboGeth/tg/chaindata --http.api=eth,debug,net
```
**For remote DB**

Run turbo-geth in one terminal window

```
> ./build/bin/tg --private.api.addr=localhost:9090
```

Run RPC daemon
```
> ./build/bin/rpcdaemon --private.api.addr=localhost:9090
```

### Test

Try `eth_blockNumber` call. In another console/tab, use `curl` to make RPC call:
````
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber", "params": [], "id":1}' localhost:8545
````
It should return something like this (depending on how far your turbo-geth node has synced):
````
{"jsonrpc":"2.0","id":1,"result":823909}
````

### For Developers

**Code generation**: `go.mod` stores right version of generators, use `mage grpc` to install it and generate code.