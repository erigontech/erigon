# txpool interface
Transaction pool is supposed to import and track pending transactions. As such, it should conduct at least two checks:
- Transactions must have correct nonce
- Gas fees must be covered

## State streaming
For transaction checks to function, the pool must also track balance and nonce for sending accounts.

On import of transactions from unknown sender, transaction pool can request balance and nonce at a particular block.

To track existing accounts, transaction pool connects to Ethereum client and receives a stream of BlockDiffs. Each of these represents one block, applied or reverted, and contains all the necessary information for transaction pool to track its accounts.

For applied blocks:
- Block's hash
- Parent block's hash
- New balances and nonces for all accounts changed in this block

For reverted blocks:
- Reverted block's hash
- New (reverted's parent) hash
- New parent (reverted's grandfather) hash
- List of reverted transactions
- Balances and nonces for all accounts changed in reverted block, at new (reverted's parent) state.

BlockDiffs must be streamed in the chain's order without any gaps. If BlockDiff's parent does not match current block hash, transaction pool must make sure that it is not left in inconsistent state. One option is to reset the transaction pool, reimport transactions and rerequest state for those senders.

## Reorg handling
Simple example:

```
A - D -- E -- F
 \
  - B -- C
```

Transaction pool is at block C, canonical chain reorganizes to F.

We backtrack to common ancestor and apply new chain, block by block.

Client must send the following BlockDiffs to txpool, in order:
- revert C to B
- revert B to A
- apply D on A
- apply E on D
- apply F on E