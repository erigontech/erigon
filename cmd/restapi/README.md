# Turbo-Geth Rest API

## Build

```
make restapi
```

## Running

* Running node with `--private.api.addr` (e.g `./build/bin/geth --private.api.addr localhost:9999`).
* Running Restapi: `./build/bin/restapi` (Default Port: 8080)

## API

* `/api/v1/remote-db/`: gives remote-db url
* `/api/v1/accounts/:accountID`: gives account data
    * accountID is account address
    * Reponse: 
    
```json
{

    "balance":"BALANCE",
    "code_hash":"HASH",
    "implementation":
        {
            "incarnation":NUMBER
        },
        "nonce":NUMBER,
        "root_hash":"HASH"      
}
```
* `/api/v1/storage/`
    * gives the storage
    * Response:
```json
[
    {"prefix": "Storage Prefix","value": "Value"},
    ...
]
```
* `/api/v1/retrace/:chain/:number`
    * chain is the name of the chain(mainnet, testnet, goerli and rinkeby)
    * number is block number (e.g 98345)
    * extract changeSets and readSets for each block
    * Response:
```json
[
    {
        "storage": {
            "reads": [READ, ...],
            "writes": [WRITE, ...]
        },
        "accounts": {
            "reads": [READ, ...],
            "writes": [WRITE, ...]
        }
    }
]
```
* `/api/v1/intermediate-hash/`
    * extract intermediate hashes
    * Response:
```json
[
    {"prefix": "Prefix","value": "Value"},
    ...
]
```

