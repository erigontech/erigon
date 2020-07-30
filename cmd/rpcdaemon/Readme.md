
## Getting Started

In order to build and run turbo-geth node together with RPC daemon, you need to do the following:
1. Clone turbo-geth repo
2. Build it by running `make`
3. Start it (it will start syncing to the mainnet) like this:
````
./build/bin/geth --private.api.addr=localhost:9999
````
4. Look out for this in the console/log file:
````
INFO [11-30|18:34:12.687] Remote DB interface listening on         address=localhost:9999
````
5. In another terminal/tab, build RPC daemon:
````
make rpcdaemon
````
6. Run it:
````
./build/bin/rpcdaemon --rpcapi eth
````
By default, it will connect to the turbo-geth node on the `localhost:9999`, but this can be changed via command line parameters. Note that it does not matter in which order you start these two processes, RPC daemon will only try to connect to turbo-geth node when serving its first RPC request, and then it will reconnect if connection is lost (for example, if you restart turbo-geth)
7. Try `eth_blockNumber` call. In another console/tab, use `curl` to make RPC call:
````
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber", "params": [], "id":1}' localhost:854
````
8. It should return something like this (depending on how far your turbo-geth node has synced):
````
{"jsonrpc":"2.0","id":1,"result":823909}
````