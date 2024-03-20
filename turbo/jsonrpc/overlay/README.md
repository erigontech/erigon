# Overlays
Overlays allow you to add your custom logic to already deployed contracts and simulate events and calls on top of them.
With overlays you can create new view functions, modify existing ones, change field visibility, emit new events and query the historical data of many contracts with your modified source code.

## API
This explains how to use the overlay API.

### `overlay_callConstructor`
This method needs to be called once with the new bytecode.

It first does a lookup of the creationTx for the given contract.
Once it's found, it injects the new code and returns the new creation bytecode result from the EVM to the caller.

Example request:
```json
{
  "id" : "1",
  "jsonrpc" : "2.0",
  "method" : "overlay_callConstructor",
  "params" : [
    "<CONTRACT_ADDRESS>",
    "<BYTECODE>"
  ]
}
```

Example response:
```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "code": "<CREATION_BYTECODE>"
  }
}
```

### `overlay_getLogs`
This method can be called multiple times to receive new logs from your new bytecode.

It has the same interface as `eth_getLogs` but it also accepts state overrides as the second param.
We can pass the creation bytecode from the call to `overlay_callConstructor` along to `overlay_getLogs` as state overrides.
The passed block range for the filter defines the initial block range that needs to be replayed with the given state overrides.
Once all blocks are replayed, the logs are returned to the caller.

Example request:
```json
{
   "id" : 1,
   "jsonrpc" : "2.0",
   "method" : "overlay_getLogs",
   "params" : [
      {
         "address" : "<CONTRACT_ADDRESS>",
         "fromBlock" : 19470165,
         "toBlock" : 19478165
      },
      {
         "<CONTRACT_ADDRESS>" : {
            "code" : "<CREATION_BYTECODE>"
        }
      }
    ]
}
```

Example response as in `eth_getLogs`:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "address": "<CONTRACT_ADDRESS>",
      "topics": [
        "0x8b615fdc5486fad0275d26c56169e31fd7a71d8f916bb2e9ba80b626903a8b64",
        "0x0000000000000000000000006f3a86a0fd7aafa0b679d4dac8ea7dfccce383ab",
        "0x0000000000000000000000006f3a86a0fd7aafa0b679d4dac8ea7dfccce383ab",
        "0x0000000000000000000000008e27d64063d74c7c2f7c8609e5b6d78d03378d23"
      ],
      "data": "0x0000000000000000000000000000000000000000000001795e6d875dd7c7541500000000000000000000000000000000000000000000014611be39e4bd5d6c300000000000000000000000000000000000000000000000294b9e341f9bf78418",
      "blockNumber": "0x1293695",
      "transactionHash": "0xbdc424f1b17589e7b0ea169f65f2a8e30fee612acc0560db350f42ec26bd1f87",
      "transactionIndex": "0x7e",
      "blockHash": "0xf5b1c1783f3a0e0e5a26e862da7b77d07f6d7ba01da74182af0fc432cc62e404",
      "logIndex": "0x176",
      "removed": false
    }
  ]
}
```

### `eth_call`
This method can be called multiple times to call new view functions that you defined in your new bytecode.

By sending the creation bytecode received from `overlay_callConstructor` as state overrides to `eth_call` you'll be able to call new functions on your contract.

## Optimizations
In general, a contract rarely touches every single block unless it's a very popular contract. There's an optimization that uses a bitmap check with
the existing db indexes for the `kv.CallFromIndex` and `kv.CallToIndex` to identify blocks which can be safely skipped.
This optimization makes ad-hoc simulations performant for block ranges of 10k or more, without the need of any back-filling infra for indexing.

The overall processing time was reduced significantly for contracts which are not "busy". For contracts which are touching nearly every block, it won't make a huge difference.

## Tests
There's a [postman collection for overlays](../../../cmd/rpcdaemon/postman/Overlay_Testing.json) with sample requests for `overlay_callConstructor` and `overlay_getLogs` which can be used for reference and refactorings.

## Configuration
- add `ots,overlay` to your `--http.api` flag
- increase the concurrent db reads because overlays are computed in parallel `--db.read.concurrency=256`