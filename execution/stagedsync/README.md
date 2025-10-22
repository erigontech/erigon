# Staged Sync

Staged Sync is a version of [Go-Ethereum](https://github.com/ethereum/go-ethereum)'s Full Sync that was rearchitected for better performance.

It is I/O intensive and even though we have a goal of being able to sync the node on an HDD, we still recommend using fast SSDs.

Staged Sync, as its name suggests, consists of 10 stages that are executed in order, one after another.

## How The Sync Works

For each peer Erigon learns what the HEAD blocks is and it executes each stage in order for the missing blocks between the local HEAD block and the peer's head blocks.

The first stage (downloading headers) sets the local HEAD block.

Each stage is executed in order and a stage N does not stop until the local head is reached for it.

That means, that in the ideal scenario (no network interruptions, the app isn't restarted, etc), for the full initial sync, each stage will be executed exactly once.

After the last stage is finished, the process starts from the beginning, by looking for the new headers to download.

If the app is restarted in between stages, it restarts from the first stage.

If the app is restarted in the middle of the stage execution, it restarts from that stage, giving it the opportunity to complete.

### How long do the stages take?

Here is a pie chart showing the proportional time spent on each stage (it was
taken from the full sync). It is by all means just an estimation, but it gives
an idea.

![Full sync breakdown](/docs/assets/stagedsync_proportions.png)

## Reorgs / Unwinds

Sometimes the chain makes a reorg and we need to "undo" some parts of our sync.

This happens backward from the last stage to the first one with one caveat that txn pool is updated after we already unwound the execution so we know the new nonces.

That is the example of stages order to be unwound (unwind happens from right to left).

```golang
state.unwindOrder = []*Stage{
        // Unwinding of txn pool (reinjecting transactions into the pool needs to happen after unwinding execution)
        stages[0], stages[1], stages[2], stages[9], stages[3], stages[4], stages[5], stages[6], stages[7], stages[8],
}
```

## Preprocessing with [ETL](https://github.com/erigontech/erigon/tree/main/db/etl)

Some stages use our ETL framework to sort data by keys before inserting it into the database.

That allows to reduce db write amplification significantly.

So, when we are generating indexes or hashed state, we do a multi-step process.

1. We write the processed data into a couple of temp files in your data directory;
2. We then use a heap to insert data from the temp files into the database, in the order that minimizes db write amplification.

This optimization sometimes leads to dramatic (orders of magnitude) write speed improvements.

## What happens after the Merge?

In the Proof-of-Stake world staged sync becomes somewhat more complicated, as the following diagram shows.
![Staged Sync in PoS](/docs/assets/pos_downloader.png)

## Stages (for the up to date list see [`stages.go`](/execution/stagedsync/stages/stages.go) and [`stagebuilder.go`](/execution/stagedsync/stagebuilder.go))

Each stage consists of 2 functions `ExecFunc` that progresses the stage forward and `UnwindFunc` that unwinds the stage backwards.

Most of the stages can work offline though it isn't implemented in the current version.

We can add/remove stages, so exact stage numbers may change - but order and names stay the same.

### Stage 1: [Snapshots](/execution/stagedsync/stage_snapshots.go)

Download Snapshots (segments)

### Stage 2: [Block Hashes](/execution/stagedsync/stage_blockhashes.go)

Creates an index of blockHash -> blockNumber extracted from the headers for faster lookups and making the sync friendlier for HDDs.

### Stage 3: [Recover Senders Stage](/execution/stagedsync/stage_senders.go)

This stage recovers and stores senders for each transaction in each downloaded block.

This is also a CPU intensive stage and also benefits from multi-core CPUs.

This stage doesn't use any network connection.

### Stage 4: [Execute Blocks Stage](/execution/stagedsync/stage_execute.go)

During this stage, we execute block-by-block everything that we downloaded before.

One important point there, that we don't check root hashes during this execution, we don't even build a merkle trie here.

This stage is single threaded.

This stage doesn't use internet connection.

This stage is disk intensive.

This stage can spawn unwinds if the block execution fails.

Also at this stage we computing MerkleTrie Root (also we call it Commitment)

Also at this stage we writing: **Account History Index**, **Storage History Index**, **Log Index**, **Call traces index**

### Stage 5: Tx Lookup Index

This index sets up a link from the transaction hash to the block number.

### Stage 6: Finish

This stage sets the current block number that is then used by [RPC calls](../../cmd/rpcdaemon/README.md), such as [`eth_blockNumber`](../../README.md).
