# Staged Sync

Staged Sync is a version of [Go-Ethereum](https://github.com/ethereum/go-ethereum)'s Full Sync that was rearchitected for better performance.

It is I/O intensive and even though we have a goal on being able to sync the node on an HDD, we still recommend using fast SSDs.

Staged Sync, as its name suggests, consists of 10 stages that are executed in order, one after another.

## How The Sync Works

For each peer Erigon learns what the HEAD blocks is and it executes each stage in order for the missing blocks between the local HEAD block and the peer's head blocks.

The first stage (downloading headers) sets the local HEAD block.

Each stage is executed in order and a stage N does not stop until the local head is reached for it.

That mean, that in the ideal scenario (no network interruptions, the app isn't restarted, etc), for the full initial sync, each stage will be executed exactly once.

After the last stage is finished, the process starts from the beginning, by looking for the new headers to download.

If the app is restarted in between stages, it restarts from the first stage.

If the app is restarted in the middle of the stage execution, it restarts from that stage, giving it the opportunity to complete.

### How long do the stages take?

Here is a pie chart showing the proportional time spent on each stage (it was
taken from the full sync). It is by all means just an estimation, but it gives
an idea.

![](/docs/stagedsync_proportions.png)

## Reorgs / Unwinds

Sometimes the chain makes a reorg and we need to "undo" some parts of our sync.

This happens backward from the last stage to the first one with one caveat that tx pool is updated after we already unwound the execution so we know the new nonces.

That is the example of stages order to be unwound (unwind happens from right to left).

```
state.unwindOrder = []*Stage{
		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
		stages[0], stages[1], stages[2], stages[9], stages[3], stages[4], stages[5], stages[6], stages[7], stages[8],
	}
```

## Preprocessing with [ETL](https://github.com/ledgerwatch/erigon-lib/tree/main/etl)

Some stages use our ETL framework to sort data by keys before inserting it into the database.

That allows to reduce db write amplification significantly.

So, when we are generating indexes or hashed state, we do a multi-step process.

1. We write the processed data into a couple of temp files in your data directory;
2. We then use a heap to insert data from the temp files into the database, in the order that minimizes db write amplification.

This optimization sometimes leads to dramatic (orders of magnitude) write speed improvements.

## What happens after the Merge?

In the Proof-of-Stake world staged sync becomes somewhat more complicated, as the following diagram shows.
![](/docs/pos_downloader.png)

## Stages (for the up to date list see [`stages.go`](/eth/stagedsync/stages/stages.go) and [`stagebuilder.go`](/eth/stagedsync/stagebuilder.go)):

Each stage consists of 2 functions `ExecFunc` that progesses the stage forward and `UnwindFunc` that unwinds the stage backwards.

Most of the stages can work offline though it isn't implemented in the current version.

We can add/remove stages, so exact stage numbers may change - but order and names stay the same.

### Stage 1: [Download Headers Stage](/eth/stagedsync/stage_headers.go)

During this stage we download all the headers between the local HEAD and our peer's head.

This stage is CPU intensive and can benefit from a multicore processor due to verifying PoW of the headers.

Most of the unwinds are initiated on this stage due to the chain reorgs.

This stage promotes local HEAD pointer.

### Stage 2: [Block Hashes](/eth/stagedsync/stage_blockhashes.go)

Creates an index of blockHash -> blockNumber extracted from the headers for faster lookups and making the sync friendlier for HDDs.

### Stage 4: [Download Block Bodies Stage](/eth/stagedsync/stage_bodies.go)

At that stage, we download bodies for block headers that we already downloaded.

That is the most intensive stage for the network connection, the vast majority of data is downloaded here.

### Stage 5: [Recover Senders Stage](/eth/stagedsync/stage_senders.go)

This stage recovers and stores senders for each transaction in each downloaded block.

This is also a CPU intensive stage and also benefits from multi-core CPUs.

This stage doesn't use any network connection.

### Stage 6: [Execute Blocks Stage](/eth/stagedsync/stage_execute.go)

During this stage, we execute block-by-block everything that we downloaded before.

One important point there, that we don't check root hashes during this execution, we don't even build a merkle trie here.

This stage is single threaded.

This stage doesn't use internet connection.

This stage is disk intensive.

This stage can spawn unwinds if the block execution fails.

### Stage 7: [Transpile marked VM contracts to TEVM](/eth/stagedsync/stage_tevm.go)

[TODO]

### Stage 8: [Generate Hashed State Stage](/eth/stagedsync/stage_hashstate.go)

Erigon during execution uses Plain state storage.

> Plain State: Instead of the normal (we call it "Hashed State") where accounts and storage items are addressed as `keccak256(address)`, in the plain state them are addressed by the `address` itself.

Though, to make sure that some APIs work and keep the compatibility with the other clients, we generate Hashed state as well.

If the hashed state is not empty, then we are looking at the History ChangeSets and update only the items that were changed.

This stage doesn't use a network connection.

### Stage 9: [Compute State Root Stage](/eth/stagedsync/stage_interhashes.go)

This stage build the Merkle trie and checks the root hash for the current state.

It also builds Intermediate Hashes along the way and stores them into the database.

If there were no intermediate hashes stored before (that could happend during the first initial sync), it builds the full Merkle Trie and its root hash.

If there are intermediate hashes in the database, it uses the block history to figure out which ones are outdated and which ones are still up to date. Then it builds a partial Merkle trie using the up-to-date hashes and only rebuilding the outdated ones.

If the root hash doesn't match, it initiates an unwind one block backwards.

This stage doesn't use a network connection.

### Stage 10: [Generate call traces index](/eth/stagedsync/stage_call_traces.go)

[TODO]

### Stages [11, 12](/eth/stagedsync/stage_indexes.go), [13](/eth/stagedsync/stage_log_index.go), and [14](/eth/stagedsync/stage_txlookup.go): Generate Indexes

There are 4 indexes that are generated during sync.

They might be disabled because they aren't used for all the APIs.

These stages do not use a network connection.

**Account History Index**

This index stores the mapping from the account address to the list of blocks where this account was changed in some way.

**Storage History Index**

This index stores the mapping from the storage item address to the list of blocks where this storage item was changed in some way.

**Log Index**

This index sets up a link from the [TODO] to [TODO].

**Tx Lookup Index**

This index sets up a link from the transaction hash to the block number.

### Stage 15: [Transaction Pool Stage](/eth/stagedsync/stage_txpool.go)

During this stage we start the transaction pool or update its state. For instance, we remove the transactions from the blocks we have downloaded from the pool.

On unwinds, we add the transactions from the blocks we unwind, back to the pool.

This stage doesn't use a network connection.

### Stage 16: Finish

This stage sets the current block number that is then used by [RPC calls](../../cmd/rpcdaemon/README.md), such as [`eth_blockNumber`](../../README.md).
