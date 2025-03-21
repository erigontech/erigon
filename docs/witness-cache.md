# Witness Cache Flags

Enable caching of witnesses for batches. This is useful for speeding up the RPC endpoint `zkevm_getBatchWitness` as it will check the cache first before generating a new witness. The cache is stored in the DB and is truncated when a higher verified batch comes in. This cache is generated towards the end of the stage loop.

## Flag List

- `zkevm.witness-cache-enable`
- `zkevm.witness-cache-purge`
- `zkevm.witness-cache-batch-ahead-offset`
- `zkevm.witness-cache-batch-behind-offset`

***

### Flag Name
`zkevm.witness-cache-enable`

### Description
Turn on witness caching for batches. By default this will cache 5 batches behind the highest verified on the chain. This happens towards the end of the stage loop after execution. Then in the RPC endpoint zkevm_getBatchWitness we collect it from the cache if it has been cached.

### Example
`zkevm.witness-cache-enable: true`

### Default Value
`false`

### Use Case
A network may have a lot of unverified batches that need sending to the prover. To speed this up so we are not generating a full witness every rpc call, we can cache the witness in the stage loop. Then in the RPC call we check to see if that witness is cached, if it is then we can send that straight to the prover without a full generation. The node must have the flag enabled for the RPC endpoint to check the witness cache. The only RPC endpoint that will check cache is the `zkevm_getBatchWitness`. This is because the witnesses are cached exactly per batch, and not per block range.

***

### Flag Name
`zkevm.witness-cache-purge`

### Description
Purge all the witness cache from the db on node startup.

### Example
`zkevm.witness-cache-purge: true`

### Default Value
`false`

### Use Case
You may need to purge the witness cache from the DB if there is an issue that arises at any point. For this condition we have a flag to get rid of it all and start again.

***

### Flag Name
`zkevm.witness-cache-batch-ahead-offset`

### Description
How many batches we want to cache ahead of the highest verified.
Once a higher verified batch comes in we will truncate below and cache higher so you will always have the same amount of caches stored in the DB.

### Example
`zkevm.witness-cache-batch-ahead-offset: 100`

### Default Value
`0`

### Use Case
If the highest verified = 100,000 and we have the flag set to 100, we will cache batches 100,000 -> 100,100. Then now we can send all them batches to the prover without having to do a full gen on the RPC endpoint and it taking a long time.
If the highest executed batch is 100,050 then we will only cache 100,000 -> 100,050 (50 batches).

***

### Flag Name
`zkevm.witness-cache-batch-behind-offset`

### Description
how many batches we want to cache behind the highest verified batch.

### Example
`zkevm.witness-cache-batch-behind-offset: 100`

### Default Value
`5`

### Use Case
If you want to cache a few behind the highest verified so that the RPC endpoint will be faster to retrieve.