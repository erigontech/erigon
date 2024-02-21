# Overlays
Overlays allow you to add your custom logic to already deployed contracts and simulate events and calls on top of them.
With overlays you can create new view functions, modify existing ones, change field visibility, emit new events and query the historical data of many contracts with your modified source code.

## API
This explains how to use the overlay API.

### `overlay_callConstructor`
This method needs to be called once with the new bytecode.

It first does a lookup of the creationTx for the given contract.
Once it's found, it injects the new code and returns the new creation bytecode result from the EVM to the caller.

### `overlay_getLogs`
This method can be called multiple times to receive new logs from your new bytecode.

It has the same interface as `eth_getLogs` but it also accepts state overrides.
We can pass the creation bytecode from the call to `overlay_callConstructor` along to `overlay_getLogs` as state overrides.
The passed block range for the filter defines the initial block range that needs to be replayed with the given state overrides.
Once all relevant blocks (see Optimizations) are replayed the logs are returned to the caller.

### `eth_call`
This method can be called multiple times to call new view functions that you defined in your new bytecode.

By sending the creation bytecode received from `overlay_callConstructor` as state overrides to `eth_call` you'll be able to call new functions on your contract.

## Optimizations
In general, a contract rarely touches every single block unless it's a very popular contract. There's an optimization that uses a bitmap check with
the existing db indexes for the `kv.CallFromIndex` and `kv.CallToIndex` to identify blocks which can be safely skipped.
This optimization makes ad-hoc simulations performant for block ranges of 10k or more, without the need of any back-filling infra for indexing.

The overall processing time was reduced significantly for contracts which are not "busy". For contracts which are touching nearly every block, it won't make a huge difference.

## Tests
There's a [postman collection for overlays](../../cmd/rpcdaemon/postman/Overlay_Testing.json) with sample requests for `overlay_callConstructor` and `overlay_getLogs` which can be used for reference and refactorings.

## Configuration
- add `ots,overlay` to your `--http.api` flag
- increase the concurrent db reads because overlays are computed in parallel `--db.read.concurrency=256`