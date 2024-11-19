# Functionality of Transaction Pool
Starting point of the design is the description of various functions that transaction pool needs to perform

## Respond to tx fetch requests from network peers
As part of `eth/65` protocol, there are messages `GetPooledTransactions` (there is also variant `GetPooledTransactions66` that contains `RequestID`) that other peers can send to our node. These messages contain a list of transaction hashes. Our node is expected to reply with the list of transaction "bodies" (up to certain limit of reply message size). In order to compose such reply, tx pool needs to have function:
```
tx_hash => transaction
```

## JSON RPC request asking for transaction by hash
RPC method `eth_getTransactionByHash` first looks the given hash up in the database, in the TxLookup index. If nothing found, it queries the tx pool for this hash. To respond to such query, tx pool needs the same function as above:
```
tx_hash => transaction
```

## Send notifications about new pending transactions
There are RPC methods that allow subscribing to new pending transactions. First of all, we need to define what "new pending" transaction means. Our current understanding of this terminology is illustrated below.

![](/docs/assets/tx_types.png)

Out of all transaction arriving at the pool, there are those that are valid on the current state. Good test for those is: "If we were to produce a block with unlimited block gas limit, as large as possible, which transactions would we include?". This set of transactions is shown as light green circle.

Transactions that are not valid on the current state, can be split further into three categories. Firstly, those that will never become valid
unless there is some chain reorg. Secondly, those that may potentially become valid later. It can be because of their nonce that is too large, or because the balance currently on the sender account is deemed insufficient to pay for gas. These "potentially valid in the future" transactions are shown as pink circle. The rest can be thought if the space inside the "all transactions" circle but not within light green or pink circles, shown in light blue colour.

Due to limited amount of resources on the node (memory, CPU, network bandwidth), there is normally a limit on the total number or total size of the transactions that are actually stored by the node. This is shown by dark green (pending) and red (queued) circles. The rest of the transactions in the light green and pink areas are discarded.

Now the definition of a "new pending" transaction can be figured out by thinking about what happens when a new "remote" transaction arrives into the pool. It is assessed, firstly whether:
1. It can never be valid - discarded immediately
2. Is not valid currently, but can potentially be valid in the future - assigned to the pink circle
3. Is valid currently - assigned to the light green circle

Then, if the transaction is assigned into the light green circle, we need to check whether it can be included into the "pending" dark green circle. Usually, if it is more attractive (in terms of perceived mining reward) than some other transactions in the dark green circle, it can be included, and some other transactions are evicted. There might be some exceptions to that, if the pending circle is limited by total size of the transactions rather than total number of transaction. If the new transaction is quite large, and even by pushing some other, less attractive transactions, it cannot fit, it would not be included.

At the end, if the new transaction is included into the dark green "pending" circle, a notification needs to be sent.

We can express this function this way perhaps:
```
transaction, pending_pool, queued_pool => pending_pool', queued_pool' bool
```
Given a transaction and pending pool, this function produces modified pending pool or modified queued pool, as well as indication whether this transaction was added to the pending pool, in which case a notification needs to be sent.

## Retrieve pending and queued transaction

There are couple of RPC requests, such as `txpool_content` and `txpool_inspect` in go-ethereum, and `parity_pendingTransactions`, and `parity_allTransactions` open ethereum that require enumeration of pending and queued pools (dark green and red circles respectively).

We can express this as a function
```
=> pending_pool, queued_pool
```

## Select transaction for mining

Before a new block gets mined, local and "remote" pending transactions are queried from the transaction pool. Local transactions (injected by JSON RPC methods into the node) get priority over "remote" transactions. If there are no local transactions left and there is still "space" left in the block, "remote" transactions are selected from the pending pool, in order of their priority, which is usually defined by much Wei/eth miner would receive for each unit of gas by including that transaction.

Note that although in the current implementation, ALL pending transactions are first retrieved, then sorted by priority, and then selected one-by-one, this is not a functional requirement. A different implementation that selects pending transactions in the order of priority, without fully sorting them, would be suitable as well.

We can describe this function as:
```
=> local_transactions, sorted_iterator(pending_pool)
```

## Relaying transactions to other network peers

Here, the requirement is that transactions received from other peers via "gossip" messages, which are `Transactions` and `NewPooledTransactionHashes`, are relayed further. However, not all received transactions should be relayed. In order to prevent the node from becoming a part traffic amplification attacks on the network (where non-viable transactions are produced and relayed around the network, consuming bandwidth but never getting mined), only transactions from pending pool should be relayed. This requirement makes this particular function similar to the function of sending notifications about new pending transactions, i.e:

```
transaction, pending_pool, queued_pool => pending_pool', queued_pool' bool
```

## Remove all mined transactions from the pool
When a block gets completely imported and it is deemed as "the best" block at its block height, all the transactions in that block need to be removed from the transaction pool, because they cannot be included twice (it will be prevented by the incrementing nonce rule). This functionality can be described as:

```
tx_hashes, pending_pool, queued_pool, local_transactions => pending_pool`, queued_pool`, local_transactions`
```

where modified pending pool and queued pool, and local transactions do not contain given transaction hashes anymore.

In code can find production of such event by `p.newPendingTxns <-`, and handling of this event by `announcements := <-newTxns` [here](pool.go#L1429)

## Reinject transactions into the transaction pool on unwinding a block
This can be thought of a reverse operation from the one described before. When a block that was deemed "the best" of its height, is no longer deemed "the best", the transactions contained in it, are now viable for inclusion in other blocks, and therefore should be returned into the transaction pool. We can describe this function as:

```
transactions, pending_pool, queued_pool => pending_pool`, queued_pool`
```

An interesting note here is that if the block contained any transactions local to the node, by being first removed from the pool (from the "local" part of it), and then re-injected, they effective lose their priority over the "remote" transactions. In order to prevent that, somehow the fact that certain transactions were local, needs to be remembered for some time (up to some "immutability threshold")

## Question about persistence of the transactions
In the current implementation, transactions from the pool are persisted upon graceful shutdown. Upon restart, they are recovered and used for reducing the amount of reconciliation that is done via `eth/65` pooled transactions. However, if we assume that transactions in the
pool get refreshed relatively quickly, then the benefit of persistence for this particular purpose becomes uncertain.

On the other hand, if valid transactions are persisted in an append-only storage with some extra metadata, for example, time of arrival,
this could be a powerful feature for historical data analysis. If such functionality is easy to enable, it should be provided.

In any event, local transactions need to be persisted, so that they do not lose their priority when they get included into a block but then reverted.

Another challenge (solvable) of persisting transactions (for example, local transactions) is that on the start up, we need to go through the recent blocks to figure out which ones have been already included. We cannot rely on the `TxLookup` index (mapping transaction hash to block number), because it is an optional index.

## Changes for EIP-1559

![](/docs/assets/Pool-eip1559.png)

With the EIP-1559, there are a few changes to the transaction pool model described above. Now it is possible for transactions to
become executable or not without any changes to the sender's balance or nonce. This is because there is a rule that `feeCap` of a transaction needs to be no less than `baseFee` of the block, for the transaction to be includable.
Another change is that under rules of EIP-1559, transaction needs to have the balance of at least `feeCap x gasLimit`, to be includable.

# Design proposal

Given the functional analysis above and also some known challenges (for example, ensuring that transactions from the same senders are included in the strict order of their nonces, without gaps), the proposal is to have the following elements in the design: ordering function and subordinate pools, most consisting of a pair of priority queues. Description of the element follows

## Ordering function
Ordering function is a relation that introduces total order over the set of all syntactically valid transactions. This means, for any two transaction, it tells us in which order their need to be sorted.
The main idea of what such an ordering function should achieve is the stratification of transactions into sub-pools, like the ones shown above (pending, queued, etc.). For example, transactions belonging to pending sub-pool must be ordered above transactions in other sub-pools. Within pending sub-pool, the nonce increment rule needs to be enforced. Even looking at these two examples of requirements to the ordering function, we can determine that it is not enough to just look at the usual transaction attributes to order them. We'd need some supplemental data structures, and some extra "ephemeral" fields in the transactions.

First ephemeral field is an integer that acts as a bitfield, we call it `SubPool`. The bits in this field are (in the order from most significant to least significant):
1. Minimum fee requirement. Set to `1` if `feeCap` of the transaction is no less than in-protocol parameter of minimal base fee. Set to `0` if `feeCap` is less than minimum base fee, which means this transaction will never be included into this particular chain.
2. Absence of nonce gaps. Set to `1` for transactions whose nonce is `N`, state nonce for the sender is `M`, and there are transactions for all nonces between `M` and `N` from the same sender. Set to `0` is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
3. Sufficient balance for gas. Set to `1` if the balance of sender's account in the state is `B`, nonce of the sender in the state is `M`, nonce of the transaction is `N`, and the sum of `feeCap x gasLimit + transferred_value` of all transactions from this sender with nonces `N+1 ... M` is no more than `B`. Set to `0` otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
4. Dynamic fee requirement. Set to `1` if `feeCap` of the transaction is no less than `baseFee` of the currently pending block. Set to `0` otherwise.
5. Local transaction. Set to `1` if transaction is local.

Currently there are five bits in the `SubPool` ephemeral field, so it is an integer `0..31`. These integers can be directly compared, and the largest value means a transaction is more preferable. This ephemeral field allows us to stratify all transactions into three sub pools (hence the name of the field).

![](/docs/assets/Subpools.png)

There are three subpools - pending (green), basefee (yellow), and queued (red). Each subpool consists of two sorting data structures, one of which sorts transactions in the order of decreasing priority ("best" structure), another - in the order of increasing priority ("worst" structure). For yellow and red subpools, the sorting data structures are priority queues. For the green pool, the "best" structure could be priority queue, but it may be more convenient to have it as a list, to avoid frequent popping and pushing elements in and out of priority queue.
Worst data structures serve to restrict the size of a subpool. The restriction can be defined either in terms of number of transactions, or in terms of total size of transactions (in bytes). Worst data structure allows taking the "worst" elements from the pool (and either demoting them into lower pools, or discarding them), that is why it needs to order transactions from the lowest priority to the highest.
Best data structures for yellow and red subpools serve to promote transactions to higher pools. Best data structure in the green pool allows selection of a sub-set of highest priority transactions for the purposes of composing a block (mining or pending block API).

[RULES need to be updated for the additions of the local transaction bit]

The "tubes" (thick arrows) between the pools show how transactions move between the pools. Below are the rules of such movements:
1. If top element in the worst green queue has `SubPool` != `0b1111` (binary), it needs to be removed from the green pool. If `SubPool` < `0b1000` (not satisfying minimum fee), discard. If `SubPool` == `0b1110`, demote to the yellow pool, otherwise demote to the red pool.
2. If top element in the worst green queue has `SubPool` == `0b1111`, but there is not enough room in the pool, discard.
3. If the top element in the best yellow queue has `SubPool` == `0b1111`, promote to the green pool.
4. If the top element in the worst yellow queue has `SubPool` != `0x1110`, it needs to be removed from the yellow pool. If `SubPool` < `0b1000` (not satisfying minimum fee), discard. Otherwise, demote to the red pool.
5. If the top element in the worst yellow queue has `SubPool` == `0x1110`, but there is not enough room in the pool, discard.
6. If the top element in the best red queue has `SubPool` == `0x1110`, promote to the yellow pool. If `SubPool` == `0x1111`, promote to the green pool.
7. If the top element in the worst red queue has `SubPool` < `0b1000` (not satisfying minimum fee), discard.
8. If the top element in the worst red queue has `SubPool` >= `0b1000`, but there is not enough room in the pool, discard.

The rules above need to be checked once either a new transaction has appeared (it needs to be sorted into a sub-pool, then it might pop up at the top of the worst queue to be discarded, or push out another transaction), or if `baseFee` of the pending block changes. In the latter case, all priority queues need to have their invariants re-established (in Go, it is done by `heap.Init` function, which has linear algorithmic complexity). Also in the latter case, or if the new transaction ends up inhabiting the green pool, the best structure of the green pool needs to be re-sorted.

### How is `SubPool` ephemeral field calculated?
It is quite easy to imagine how to calculate the bits 1 and 4 of the `SubPool` (minimum fee requirement and dynamic fee requirement). But it is not obvious how to calculate bits 2 and 3 (absence of nonce gaps and sufficient balance).

We introduce another couple of data structures alongside the sub pools. First will simply map address of an account to a `senderId`. Here, `senderId` would be `uint64` for convenience:
```
senderIds :: address => senderId
```

Another data structure is a mapping of sender Id to:
1. Account balance in the current state (before pending block)
2. Account nonce in the current state (before pending block)
3. Sorted map of `nonce => transaction_record`

So this structure looks like:
```
senders :: senderId => { state_balance; state_nonce; nonce =>(sorted) transaction_record }
```

Here, `transaction_record` is a pointer to a piece of data that represent a transaction in one of the sub-pools.
Data structure `senders` allows us to react on the following events and modify the ephemeral field `SubPool` inside `transaction_record`:
1. New best block arrives, which potentially changes the balance and the nonce of some senders. We use `senderIds` data structure to find relevant `senderId` values, and then use `senders` data structure to modify `state_balance` and `state_nonce`, potentially remove some elements (if transaction with some nonce is included into a block), and finally, walk over the transaction records and update `SubPool` fields depending on the actual presence of nonce gaps and what the balance is.
2. New transaction arrives, and it may potentially replace existing one, shifting the balances required in all subsequent transactions from the same sender. Also, newly arrived transaction may fill the nonce gap. In both of these cases, we walk over the transaction records and update `SubPool` fields.
3. Transaction is discarded. As it will be mentioned later, the ordering function is designed in such a way that transaction with a nonce `N` can only be discarded from the sub pools if all transactions with nonces higher than `N` have been discarded. Therefore, the only reaction on discarding a transaction is the deletion of corresponding entry from the mapping `nonce => transaction_record`.

### Other parts of the ordering function

The field `SubPool` is only one part of the ordering function, the part that can be used to stratify transactions into sub pools, each having certain resource limit. Within every sub pool, there is a finer grained ordering. This ordering needs to server two objectives:
1. In the green sub pool, any part of the sorted "best" structure, as long as it starts from the best transaction, can be taken and inserted in the block. Given sufficient gas limit in that block, all transactions taken in that way, must be includable. In particular, it means that strict nonce ordering of transactions from the same sender must be observed. Also, there needs to be enough balance in the sender's account to guarantee payment for `gasLimit x feeCap + transferred_value` for every transaction.
2. In all sub pools, transactions with least priority need to show up on the top of the "worst" priority queues.

To satisfy the first objective, we need to another three ephemeral fields in each transactions.

Firstly, the cumulative required balance. It is the sum of `gasLimit x feeCap + transferred_value` for the transaction in question and all transactions from the sender that are required to be included before. The reason to have this field is to make sure we do not need to update every transaction when their sender balance change.

Secondly, the minimum `feeCap` for the transaction in question and all transactions from the sender that are required to be included before. This minimum value, and not the `feeCap` of the transaction in question is used for ordering.

Thirdly, the minimum `tip` for the transaction in question and all transactions from the sender that are required to be included before. This minimum value, and not the `tip` of the transaction in question, is used for ordering.

To illustrate the use of the fields described above, let us take an example. Given set of transactions:

| TxId | SenderId | Nonce | FeeCap | Tip |
|------|----------|-------|--------|-----|
| 1    | A        | 2     | 23     | 12  |
| 2    | A        | 3     | 45     | 10  |
| 3    | A        | 4     | 22     | 15  |
| 4    | B        | 1     | 30     | 14  |

We first compute the minimum of `FeeCap` and `Tip` for each of them, resulting in the following values:

| TxId | SenderId | Nonce | FeeCap | Tip | min(FeeCap)      | min(Tip)         |
|------|----------|-------|--------|-----|------------------|------------------|
| 1    | A        | 2     | 23     | 12  | min{23}=23       | min{12}=12       |
| 2    | A        | 3     | 45     | 10  | min{45,23}=23    | min{10,12}=10    |
| 3    | A        | 4     | 22     | 15  | min{22,45,23}=22 | min{15,10,12}=10 |
| 4    | B        | 1     | 30     | 14  | min{30}=30       | min{14}=14       |

Now, to demonstrate the use of these ephemeral fields in the ordering function, we need to pick `baseFee`. For first case, let us pick it so that all transactions can just pay full `Tip` to the miner. That value is `baseFee = 11`. In this case, so-called `effectiveTip` will be equal to `min(Tip)` for all transactions, and the ordering is as follows (from highest priority to lowest priority):

```
Tx 4 (effectiveTip 14)
Tx 1 (effectiveTip 12)
Tx 2 (effectiveTip 10, chosen over Tx 3 because of the nonce)
Tx 3 (effectiveTip 10)
```

If we ordered these transactions by `Tip` instead of `min(Tip)`, the order would have been:
```
Tx 3, Tip 15, sender A, nonce 4
Tx 4, Tip 14, sender B, nonce 1
Tx 1, Tip 12, sender A, nonce 2
Tx 2, Tip 10, sender A, nonce 3
```

this ordering is incorrect, because `Tx 3` cannot be included before both `Tx 1` and `Tx 2` are included. We could have enforced this relative ordering by comparing their nonces, like this:
```
Tx 1, Tip 12, sender A, nonce 2
Tx 2, Tip 10, sender A, nonce 3
Tx 4, Tip 14, sender B, nonce 1
Tx 3, Tip 15, sender A, nonce 4
```

but that would have been insufficient, because it would have violated the transitive property of the ordering function, which it must have to be correctly used in sorting structures. In this example we are lucky it did not cause inconsistency. But if the `Tip` of `Tx 4` were `11` instead of `14`, then we could have inserted it either before `Tx 1` (because `11 < 12`), or after `Tx 2` (because `11 > 10`). Using `min(Tip)` removes this inconsistency and is compatible with enforcing the strict ordering of the nonces.

If we now choose a slightly higher `baseFee`, lets say, `13`, for transactions `Tx 1` and `Tx 3`, `effectiveTip` becomes `min(FeeCap) - baseFee` instead of `min(Tip)` and the ordering is as follows:

```
Tx 4 (effectiveTip 14)
Tx 1 (effectiveTip 10, chosen over Tx 2 because of the nonce)
Tx 2 (effectiveTip 10)
Tx 3 (effectiveTip 9)
```

In fact, the ordering stays the same, but the `effectiveTip` changes. Still this ordering is consistent, and is compatible with enforcing strict ordering of nonces.

All three of these ephemeral fields: cumulative required balance, minimum fee cap and minimum tip, are recalculated upon arrival of new transactions from a certain sender, by walking the corresponding sorted map in the `senders` data structure.

### Nuances of ordering in the yellow sub pool

We need to consider separately the ordering of the transactions by `effectiveTip` inside the yellow sub pool. Since transactions in the yellow sub pool have their `FeeCap < baseFee`, it means that their values of `effectiveTip` calculated as above, would be negative. To avoid negative numbers, we may instead modify the ordering function for this case to order by reverse order of `min(FeeCap)` (in yellow pool, the value of `Tip` is irrelevant). Also, having learnt about using `min(FeeCap)` instead of `FeeCap`, we can now adjust the condition for the "Dynamic Fee requirement" in the `SubPool` ephemeral field. It is set to `1` when `min(FeeCap)` is no less than `baseFee` of the pending block. This means that if a transaction satisfies the dynamic fee requirement, but its required predecessors do not, it will still be sorted into the yellow pool instead of the green sub pool.

### Ordering in the red sub pool

Red sub pool contains transactions that may never be includable. And, since red sub pool has a limited amount of space, it needs to order transactions to maintain some notion of fairness to the users. Customary limitation so far in many implementation is how many transactions per unique sender can be queued (or, in our terminology, in the red sub pool). In order to approximate this limitation, but also make it more flexible (for example, if the red sub pool has plenty of space, why not keep more transactions per sender?), we should order transactions based on how far their nonces are from the state's nonce for the sender.

For example, we have two senders, `A` and `B`, and their nonces in the state are `13` and `20` respectively. Then, for these transactions:

| TxId | SenderId | Nonce |
|------|----------|-------|
| 1    | A        | 18    |
| 2    | A        | 20    |
| 3    | B        | 26    |

we first calculate the distance from their state nonce:

| TxId | SenderId | Nonce | Distance from the state nonce |
|------|----------|-------|-------------------------------|
| 1    | A        | 18    | 18-13 = 5                     |
| 2    | A        | 20    | 20-13 = 7                     |
| 3    | B        | 26    | 26-20 = 6                     |

Then, the ordering becomes:

```
Tx 1, sender A, nonce 18
Tx 3, sender B, nonce 20
Tx 2, sender A, nonce 26
```

Using the combination of the ephemeral field for "cumulative required balance" and balance of the sender in the state, we can also introduce secondary ordering (for the case where distance from the state nonce is equal). If the nonce distances are equal, we would prefer transactions for which the gap between "cumulative required balance and the balance of the sender in the state", is lower. One small complication is that such gap may well be a negative number. In such cases, we can simply take the gap to be zero.

# Testing strategy

Given the complexity of the ordering function and variety of rules for movements of transactions between the sub pools, it is likely that manually writing test cases would not be a very productive way of testing the implementation of the design proposed above. Therefore, we shall try to use fuzz testing. Generate a set of transaction pseudo-randomly. Use algorithms to sort them into the sub pools. Check invariants on the sub pools.

# Coherence of the state cache and the state reads

This applies not just to transaction pool, but also to the RPC daemon, because it will benefits from the state cache. One of the main issues with the state cache (as it is presented in the KV interface) is the coherence between what is cached and what is queried from
the remote database (over gRPC). Here is the suggestion on how to resolve it.

Firstly, it will require a change in the KV interface. Currently, the `StateChange` is defined like this:
```
message StateChange {
  Direction direction = 1;
  uint64 blockHeight = 2;
  types.H256 blockHash = 3;
...
}
```
and if we look at the implementation, we find that for the forward changes, we generate one `StateChange` message every single block. This means we can easily detect any missing blocks if gRPC transport loses a message for some reason.
But for the unwind changes, everything is delivered in one "lump". For example, if unwind goes over 3 blocks, it will produce a single `StateChange` message and it won't be possible to detect any missing messages. Therefore, this is a suggested modification:

```
message StateChange {
  uint64 prevBlockHeight = 1;
  uint64 blockHeight = 2;
  types.H256 prevBlockHash = 3;
  types.H256 blockHash = 4;
...
}
```
Firstly, we now specified what was the expected block height and the block hash on top of which this state change should be applied. This makes sense because any such `StateChange` leads to valid (uncorrupted) state only if applied to specific previous state. Secondly, the indication of "direction" is not required anymore, because it should be obvious from the relationship between `prevBlockHeight` and `blockHeight`.

Having this modified interface, it would be possible to batch the forward updates for multiple blocks into one. But perhaps more importantly, it allows the "client side" (tx pool, or RPC daemon, or other system where state cache is used) to detect the lost messages, and invalidate the state cache if it happens.

![](/docs/assets/Coherence.png)

State Cache works on top of Database Transaction and pair Cache+ReadTransaction must provide "Serializable Isolation Level" semantic: all data form consistent db view at moment when read transaction started, read data are immutable until end of read transaction, reader can't see newer updates.

When the stream of state updates come, we maintain an ordered list of their identifiers (can be combination/concatenation of block height and block hash). For each element in such list, we keep the pointer to the cache (for example, if the cache is represented as google B-Tree, we can keep `*BTree` pointer for each identifier, even though the majority of cache content will be shared). To ensure the synchronisation between the cache and the remote state query (via read-only transactions), we also have a map of conditional variables, with the keys being the identifiers. These conditional variables are placed into the map by read-only transactions (to wait for corresponding cache entry), and notified and removed from the map by the arrival of state updates.

Every time a new state change comes, we do the following:
1. Check that `prevBlockHeight` and `prevBlockHash` match what is the top values we have, and if they don't we invalidate the cache, because we missed some messages and cannot consider the cache coherent anymore.
2. Clone the cache pointer (such that the previous pointer is still accessible, but new one shared the content with it), apply state updates to the cloned cache pointer and save under the new identified made from `blockHeight` and `blockHash`.
3. If there is a conditional variable corresponding to the identifier, remove it from the map and notify conditional variable, waking up the read-only transaction waiting on it.

On the other hand, whenever we have a cache miss (by looking at the top cache), we do the following:
1. Once read the current block height and block hash (canonical) from underlying db transaction (for example by golang.org/x/sync/singleflight, or remoteKv server may send us this data when we open Tx)
2. Construct the identifier from the current block height and block hash
3. Look for the constructed identifier in the cache. If the identifier is found, use the corresponding cache in conjunction with this read-only transaction (it will be consistent with it). If the identifier is not found, it means that the transaction has been committed in Erigon, but the state update has not arrived yet (as shown in the picture on the right). Insert conditional variable for this identifier and wait on it until either cache with the given identifier appears, or timeout (indicating that the cache update mechanism is broken and cache is likely invalidated).

TODO: Describe the cleanup of the cache identifiers, describe exact the locking of the map of cache pointers and the map of conditional variables. Perhaps there just need to be a single map of structs like that:
```
type CacheRoot struct {
   cache *btree.BTree
   lock sync.RWMutex
   cond *sync.Cond
}
var roots map[string]CacheRoot
var rootsLock sync.RWMutex
```

where the keys in the map are concatenation of `blockHeight` (as [8]byte) and `blockHash`.
