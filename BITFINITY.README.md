# Introduction

Block importer is a process that periodically polls the blocks from the EVMC canister and writes it into a file-based erigon db. It can be used to launch a blockhain explorer that introspects the EMVC blockchain.

The code of the blockimporter is in `cmd/blockimporter`.

# Archiving historical data

```sh
blockimporter [--evm <JSON_RPC_URL>] [--db <DATABASE_PATH>] [--secondary-blocks-url <PATH TO A SECONDARY SOURCE OF BLOCKS>] [--save-history-data]
```

When `--save-history-data` flag is specified blockimporter stores history data that allows accessing the state at the given block.

# Serving rpcdaemon (EVM JSON RPC)

You can use rpcdaemon to setup an endpoint with JSON RPC server to access the blockchain state from blockimporter.

To build the project just run in the project folder.

```sh
git checkout origin/evmc_importer
make
```

The binary artifacts can be found in `build/bin` folder.

Now you can run two processes together sharing the same Db path:

```sh
build/bin/blockimporter --evm <JSON_RPC_URL> --db <DB_PATH> &\
build/bin/rpcdaemon --datadir <DB_PATH> --http.corsdomain * --http.api=eth,erigon,ots
```

In this case the JSON RPC API can be accessed by address localhost:8545 (which is a default setting for the `rpcdaemon` that can be changed by passing `--http.port` argument). For more options run `build/bin/rpcdaemon --help`

# Setting up the otterscan block explorer

Otterscan can be run with `blockimporter` using the integration via rpcdaemon:

```sh
build/bin/blockimporter --evm <JSON_RPC_URL> --db <DB_PATH> &\
build/bin/rpcdaemon --datadir <DB_PATH> --http.corsdomain * --http.api=eth,erigon,ots &\
docker run --rm -p 5100:80 --name otterscan -d --env ERIGON_URL=localhost:8545 otterscan/otterscan:v1.29.0
```

Another option is to us the docker-compose file:

```sh
cd docker
mkdir ./db
chmod 777 ./db
docker-compose up
```
