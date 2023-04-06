# Introduction

Block importer is a process that periodically polls the blocks from the EVMC canister and writes it into a file-based erigon db. It can be used to launch a blockhain explorer that introspects the EMVC blockchain.

# Usage

blockimporter [--evm <EVMC_CANISTER_URL>] [--db <DATABASE_PATH>]

# Running Ottrerscan with blockimporter

Otterscan can be run with `blockimporter` using the integration via rpcdaemon:

```
blockimporter --evm <EVMC_CANISTER_URL> --db <DB_PATH> &\
rpcdaemon --datadir <DB_PATH> --http.corsdomain * --http.api=eth,erigon,ots &\
docker run --rm -p 5100:80 --name otterscan -d --env ERIGON_URL=localhost:8545 otterscan/otterscan:v1.29.0
```

Another option is to us the docker-compose file:

```
cd docker
mkdir ./db
chmod 777 ./db
docker-compose up
```