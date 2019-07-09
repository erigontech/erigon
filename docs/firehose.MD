# Firehose Sync

Jason Carver, Brian Cloutier, Andrew Ashikhmin

**v0.3**

---
See [Firehose Sync v0.1](https://notes.ethereum.org/eXnqtO_vQquzrFDPHjuaFQ) for goals, background, and summaries of similar proposals.

See [Firehose Sync v0.2](https://notes.ethereum.org/0v_W4E8lROazqYymPAF7Ew) for the previous revision.

---

Changes in this revision:
- Dropped "fuzzy" flag
- Renamed `GetStateData` -> `GetStateRanges`, `GetStateNodeData` -> `GetStateNodes`, `GetStorageData` -> `GetStorageRanges`, `GetStorageNodeData` -> `GetStorageNodes`
- Prefix encoding changed to the Yellow Paper's
- Single block (hash) per request in `GetStateRanges` and `GetStateNodes` rather than multiple root hashes
- Return available block hashes rather than state roots
- `StateRanges` & `StorageRanges` may only have all subtrie leaves or nothing, not intermediate nodes
- Prefixes are not omitted from returned leaf keys
- Added `GetStorageSizes` & `StorageSizes`
- Accept either raw (20 byte) addresses or their hashes
- Add account address (hash) to `GetBytecode`

## Casual Examples of Firehose Sync

The following aims to give intuition of different options for how you might use Firehose, but skips the gritty details. Navigate down to [Commands Spec](#Command-Specification-for-Firehose-Sync) if you hate intuition.

In the following examples, `client` means the node that is trying to sync up, and `server` is the node that is providing the sync data to the `client`.

### Beam Sync

In this example, a client would like to run as a full node and execute transactions in a block, but has only downloaded the headers so far. It starts executing transactions, retrieving any missing state on-demand.

1. Extract the sender of the 1st transaction, check for available balance
3. There is no local state, so the storage errors out
4. Request the chunk of accounts near the sender, and re-execute the transaction
5. Look up the account of the recipient, it was not in the chunk from 3, so it errors
6. Request the chunk of accounts near the recipient, and re-execute the transaction
8. Repeat for every transaction, including EVM execution. Pause & request every time the EVM is missing data.
9. Keep all requested and written data, to request less and less data in each block
10. If blocks are executing without fault, but trie is incomplete, request data in background

This has the significant benefit that the client is asking the server for data that is very likely to be hot in cache. (Since the server itself had to recently load the data to execute the blocks)

Drilling in: what does it look like to request a chunk of addresses near the one you are interested in?

### Bulk Download

Instead of Beam Sync, the client could request all account data up front before executing transactions. Like current Fast Sync, it would wait until all data is available before starting to execute transactions.

A commonly proposed approach is to permit different prefixes to be retrieved from different state roots. The client would then follow up with a fast sync to "fix up" the missing or old data.

After bulk downloading the data, the client builds a speculative trie with the data. Almost certainly the state root of the built tree doesn't match the state root of the latest header.

1) **Client** *Looks up state root of recent header, `0xf9e8`, which doesn't match current trie*
1) **Client** Give me the top three layers of the tree at root `0xf9e8`...
3) **Server** Yup, here are your 273 nodes.
6) **Client** Can you send nodes for prefixes `0x123` and `0xfed`? They don't match my local trie.
7) **Server** Sure, have some nodes
8) repeat until ...
9) **Client** Sweet. can I have nodes for prefixes `0x123456` and `0xfedcba`?
10) **Server** You bet. If you inspect them, you'll find that they are all leaf nodes
11) **Client** Verifies locally that the full trie matches the state root, and can begin txn execution


### Flexibility

Firehose Sync *enables* the strategies above, but does not enforce any one of them on you. You could choose to guess at the preferred depth and skip verifying the root hash. Or you could attempt to sync the parts of the trie that seem static first so that you spend less time fixing up parts that are "hot". You could even use it as a kind of light client service.

## Command Specification for Firehose Sync

All the new operatives/commands live under a new [RLPx](https://github.com/ethereum/devp2p/blob/master/rlpx.md) capability, `firehose`, rather than `eth`.

### `GetStateRanges` Request (`0x00`)

The requester asks for state data (i.e. accounts) as of a given block that match a given key prefix.
This command exclusively deals with retrieving the account RLPs.

The requests can be batched so that multiple prefixes may be requested.

```
(
    # A request id is supplied, to match against the response
    id, # int
    
    # The hash of the block as of which we request the data
    block_hash, # 32 bytes

    # A list of prefixes being requested
    (
        key_prefix_0, # int; see below re. prefix encoding
        ...
    )
)
```

#### Key Prefixes Note

We use the same prefix encoding as described in Appendix C of the [Yellow Paper](https://ethereum.github.io/yellowpaper/paper.pdf#appendix.C),
also known as the compact encoding.
No additional flags are utilized (i.e. `t == 0` always).

#### Example
Say you want the accounts that (after hashing) have the prefixes `0xbeef` and `0x0ddba11`, at block `0xaaa…aaa`.
Your request would look like:
```
(
    1, #id

    0xaaa...aaa, # block hash
    
    # even-length 0xbeef and odd-length 0x0ddba11, encoded
    (
        0x00beef,
        0x10ddba11
    )
)
```


### `StateRanges` Response (`0x01`)

For each requested prefix the responder has the option of returning either:
1) **All** the keys & values that are stored in that key prefix range, OR
2) An error status code

```
(
    # The ID to be used by the requestor to match the response
    id, # int
    
    # The response data, in the same ordering of the request
    (
        # There is one 2-tuple for every prefix requested
        (
            # 0 – success
            # 1 – no data as of the requested block, the final entry of the response should be non-empty
            # 2 – too many leaves matching the prefix
            status_code, # int

            # If success, return the key/value pairs.
            # Otherwise, return an empty list.
            (
                # A single key/value pair
                (
                    # The prefix is NOT omitted from the key
                    key, # 32 bytes
                    
                    # The RLP-encoded value stored in the trie
                    value
                ),
                ...
            )
        ),
        ...
    ),

    # If an old state root was requested, return the available blocks.
    # Otherwise, return an empty list.
    (
        # include the hash of every block that the server has the data for
        available_block_0, # 32 bytes
        ...
    )
)
```

#### Response Constraints

The response must reuse the same ordering of prefixes as used for the request.

For prefixes with the success status the response **must** include all the keys and values in the prefix, such that rebuilding the sub-trie produces the hash matching that of the requested block.

"Too many leaves" (`status_code = 2`) may only be returned is there are more than 4096 leaves matching the prefix.

The keys and values within a prefix may be returned in any order; the same for available blocks.

#### Example
A response to the example request above might look like:

```
(
    1, # id
    
    # This is a list of two elements, matching the `prefixes` request example
    (
        # This 2-tuple is for the 0xbeef prefix
        (
            # the prefix was too short
            2, # status code
            
            # empty since not success
            ()
        ),
        
        # This 2-tuple is for the 0x0ddba11 prefix
        (
            # success
            0, # status code
            
            # This is the list of the keys and values under 0x0ddba11
            (
                # First key/value pair
                (0x0ddba119876..., 0x111...),
                
                # Second key/value pair
                (0x0ddba11a1b5..., 0x1a2...)
            )
        )
    ),
    
    # this is empty, because the server had data for the requested block
    () # available blocks
)
```

If the server does not have any data for the requested block, then it will return status code 1 for every prefix requested, and a list of available block hashes, like:

```
(
    1, # id

    # This is a list of two elements, matching the `prefixes` request example
    (
        # This 2-tuple is for the 0xbeef prefix
        (
            # the data as of the requested block is unavailable
            1, # status code

            # empty since not success
            ()
        ),

        # This 2-tuple is for the 0x0ddba11 prefix
        (
            # the data as of the requested block is unavailable
            1, # status code

            # empty since not success
            ()
        )
    ),

    # this should be populated, because the server did not have data for the requested block
    (
        # one available block hash
        0x1a2b3c4d...,
        # another available block hash
        0x11223344...
    ) # available blocks
)
```

The only reason a server might return status code 1 and an empty list of available blocks is if it has not synced state data yet.

It *is* possible for a server to return status code 1 for one prefix and status code 0 (or 2) for another prefix on the same block. It might do this if the server is in the middle of syncing data.

### `GetStorageRanges` Request (`0x02`)

Very similar to `GetStateRanges`: request storage data by storage root hash and key prefix.
Also include account hash for clients that find this helpful for indexing.
This command exclusively deals with retrieving the storage values.

The requests can be batched so that multiple prefixes and multiple root hashes may be requested.

```
(
    # A request id is supplied, to match against the response
    id, # int

    # Requests are batched in a list
    (
        # Each list item is a three-element tuple
        (
            # The first element is the account address or the hash thereof
            account_address, # 20 or 32 bytes
            
            # The second element is the hash of the requested storage trie root
            storage_root_hash, # 32 bytes
            
            # The third element is a list of prefixes being requested
            (key_prefix, ...)
        ),
        ...
    )
)
```


#### Example
Say you want the storage keys that (after hashing) have the prefixes `0xbeef` and `0x0ddba11`, at storage root `0xbbb...bbb` (in hashed account address `0xaaa...aaa`).
Additionally, you want all the values in the storage trie with root `0xddd...ddd` (in account address `0xbb9bc244d798123fde783fcc1c72d3bb8c189413`).
Your request would look like:
```
(
    1, # id
    
    (
        (
            # first account address hash (32 bytes)
            0xaaa...aaa,
            
            # first storage root hash
            0xbbb...bbb,
            
            # even-length 0xbeef and odd-length 0x0ddba11, encoded
            (0x00beef, 0x10ddba11)
        ),
        (
            # second account address (20 bytes)
            0xbb9bc244d798123fde783fcc1c72d3bb8c189413,
            
            # second storage root hash
            0xddd...ddd,
            
            # ask for all values by requesting the empty prefix
            (0x00)
        )
    )
)
```

Note that the syncing client can (and probably should!) skip over storage tries with duplicate roots. There is no need to request an identical storage root twice for two different accounts. This could save you about [480MB of downloads](https://notes.ethereum.org/c/r1ESS_oLE/%2FGCePmXL9TZGzZ_Cm26qxAQ) in the current mainnet.


### `StorageRanges` Response (`0x03`)


For each requested prefix the responder has the option of returning either:
1) **All** the keys & values that are stored in that key prefix range, OR
2) An error status code

This looks similar to the `StateRanges` response:

```
(
    # The ID to be used by the requestor to match the response
    id, # int
    
    # The response data, in the same ordering of the request
    (

        # there is one element in this outer list for each storage root requested
        (
        
            # There is one 2-tuple for every prefix requested
            (
    
                # 0 – success
                # 1 – no data for the requested storage root, the final entry of the response should be non-empty
                # 2 – too many leaves matching the prefix
                status_code, # int
                    
                # If success, return the key/value pairs.
                # Otherwise, return an empty list.
                (
                    # A single key/value pair
                    (
                        # The prefix is NOT omitted from the key
                        key,
                        
                        # The RLP-encoded value stored in the trie
                        value
                    ),
                    ...
                )
            ),
            ...
        ),
        ...
    ),

    # If an old storage root was requested, return the available blocks.
    # Otherwise, return an empty list.
    (
        # include the hash of every block that the server has the data for
        available_block_0, # 32 bytes
        ...
    )
)
```

#### Example
A response to the example request above might look like:

```
(
    1, # id
    
    # This is a list of two elements, matching the `prefixes` request example
    (
    
        # All values in this first list correspond to
        # storage root hash: 0xbbb...bbb
        (
            # This 2-tuple is for the 0xbeef prefix
            (
                2, # the prefix was too short
                
                # The second element must be empty if not success
                ()
            ),
            
            # This 2-tuple is for the 0x0ddba11 prefix
            (
                0, # success
                
                # This is the list of the key suffixes and values under 0x0ddba11
                (
                    # First key/value pair
                    (0x0ddba11987..., 0x111e...),
                    
                    # Second key/value pair
                    (0x0ddba110a1..., 0x1a20...)

                    # Third key/value pair
                    (0x0ddba11f43..., 0x3434...)
                )
                # Note that returned key/value pairs may be not sorted by key
            )
        ),
        
        # All values in this second list correspond to
        # storage root hash: 0xddd...ddd
        (
            # The first (only) element in this list is for the empty prefix    
            (
                1, # no data

                () # empty since not success
            )
        )
    ),
    
    # this should be populated, because the server did not have data for some requested storage roots
    (
        # one available block hash
        0x1a2b3c4d...,
        # another available block hash
        0x11223344...
    ) # available blocks
)
```


### `GetStateNodes` Request (`0x04`)

Request state trie nodes as of a specific block.
Note that this operative is similar to `GetNodeData` [eth/63], but uses prefixes rather than hashes as node keys.
It will also only return nodes from the state trie, not arbitrary node data.

In a radix trie there is a trivial one-to-one correspondence between nodes and prefixes.
Since Ethereum employs a modified radix trie with extension nodes,
we define node’s prefix as the shortest one such that all leaves descending from the node match the prefix
and, conversely, all leaves that match the prefix descend from the node in question.
If a prefix is in the middle of a Patricia extension,
the server may return either the matching node or an empty string.

```
(
    # A request id is supplied, to match against the response
    id, # int
    
    # The hash of the block as of which we request the data
    block_hash, # 32 bytes

    # A list of prefixes being requested
    (
        node_prefix_0, # int
        ...
    )
)
```

### `StateNodes` Response (`0x05`)

```
(
    # The ID to be used by the requestor to match the response
    id: int,
    
    # The response data, in the same ordering of the request
    # An empty list signifies lack of data,
    # in which case available blocks (the last element of a reply) should be populated.
    (
        # A node is returned for each requested prefix
        node_0, # RLP-encoded
        ...
    ),

    # If an old or not yet received block was requested, return the available blocks.
    # Otherwise, return an empty list.
    (
        # include the hash of every block that the server has the data for
        available_block_0, # 32 bytes
        ...
    )
)
```

Node position in the response list must correspond to the prefix position in the request list.
Empty node responses are permitted if the server declines to include it.
The list may be short or empty, if any trailing nodes are omitted.
In such cases available blocks should be populated.

### `GetStorageNodes` Request (`0x06`)

Analogue of `GetStateNodes` for storage nodes.

```
(
    # A request id is supplied, to match against the response
    id, # int
    
    # Requests are batched in a list
    (
    
        # Each list item is a three-element tuple
        (
            # The first element is the account address or the hash thereof
            account_address, # 20 or 32 bytes
            
            # The second element is the hash of the requested storage trie root
            storage_root_hash, # 32 bytes
            
            # The third element is a list of prefixes being requested
            (node_prefix, ...)
        ),
        ...
    )
)
```

### `StorageNodes` Response (`0x07`)


```
(
    # The ID to be used by the requestor to match the response
    id, # int
    
    # The response data, in the same ordering of the request
    (

        # there is one inner list for each storage root requested
        (
        
            # A node is returned for each requested key prefix
            node, # RLP-encoded
            
            ...
        ),
        ...
    ),

    # If an old storage root was requested, return the available blocks.
    # Otherwise, return an empty list.
    (
        # include the hash of every block that the server has the data for
        available_block_0, # 32 bytes
        ...
    )
)
```

Node position in the response list must correspond to the prefix position in the request list.
Empty node responses are permitted if the server declines to include it.
The list may be short or empty, if any trailing nodes are omitted.
In such cases available blocks should be populated.


### `GetBytecode` Request (`0x08`)


Just like `GetNodeData` [eth/63], except:
1. Includes a request ID
2. May only request bytecode, not arbitrary nodes data
3. Includes account information since some Ethereum clients (e.g. Turbo-Geth) use it for indexing.


```
(
    # The ID to be used by the requestor to match the response
    id, # int
    
    # Requests are batched in a list
    (

        # Each list item is a two-element tuple
        (
            # The first element is the account address or the hash thereof
            account_address_0, # 20 or 32 bytes
            
            # The second element is the hash of the requested bytecode
            bytecode_hash_0 # 32 bytes
        ),
        ...

    )
)
```

### `Bytecode` Response (`0x09`)


Just like `NodeData` [eth/63], except:
1. Includes a request ID
2. Will only return bytecode with the corresponding hash, not arbitrary nodes data


```
(
    # The ID to be used by the requestor to match the response
    id, # int
    
    # Requested bytecodes
    (
    
        # arbitrary byte length bytecode
        bytecode_0,
        
        ...
    )
)
```

Bytecode position in the response list must correspond to the position in the request list. Empty bytecode responses are permitted if the server declines to include it. The list may be short or empty, if any trailing bytecodes are omitted.


### `GetStorageSizes` Request (`0x0a`)

Request storage trie sizes (the number of leaves).

```
(
    # A request id is supplied, to match against the response
    id, # int
    
    # Requests are batched in a list
    (
    
        # Each list item is a two-element tuple
        (
            # The first element is the account address or the hash thereof
            account_address_0, # 20 or 32 bytes
            
            # The second element is the hash of the requested storage trie root
            storage_root_hash_0 # 32 bytes
        ),
        ...
    )
)
```


### `GetStorageSizes` Response (`0x0b`)

```
(
    # The ID to be used by the requestor to match the response
    id, # int
    
    # The response data, in the same ordering of the request
    (
        # An empty string may be returned if the server doesn't have the data for a requested storage.
        # In such case available blocks (the last element of a reply) should be populated.
        num_leaves_0, # int OR ""

        ...
    ),

    # If an old storage root was requested, return the available blocks.
    # Otherwise, return an empty list.
    (
        # include the hash of every block that the server has the data for
        available_block_0, # 32 bytes
        ...
    )
)
```


The server may return approximate storage sizes.
Refer to
[[Size Estimation](https://medium.com/@akhounov/estimation-approximate-of-the-size-of-contracst-in-ethereum-4642fe92d6fe)]
for a fast way of size estimation.


## Strategies for Firehose Sync

### Request Strategies

Nothing about this protocol requires or prevents the following strategies, which clients might choose to adopt:

#### Beam Sync Strategy

Instead of trying to sync the whole state up front, download state guided by the recent headers. In short: pick a very recent header, download transactions, and start running them. Every time the EVM attempts to read state that is not present, pause execution and request the needed state. After running all the transactions in the block, select the canonical child header, and repeat.

Some benefits of this strategy:
- Very short ramp-up time to running transactions locally
- Encourages cache alignment:
    - Multiple clients make similar requests at similar times, encouraging cache hits
    - Servers can predict inbound requests
    - Servers likely have the data warm in cache, by virtue of executing transactions
    - Not only are specific accounts more likely to be requested; those accounts are requested at particular state roots

It's entirely possible that it will take longer than the block time to execute a single block when you have a lot of read "misses." As you build more and more local data, you should eventually catch up.

What happens if the amount of state read in a single block is so big that clients can't sync state before servers prune the data? A client can choose a new recent header and try again. Though, it's possible that you would keep resetting this way indefinitely. Perhaps after a few resets, you could fall back to a different strategy.

Note that this may not actually complete sync as quickly as other strategies, but completing sync quickly becomes less important. You get all the benefits of running transactions locally, very quickly.

#### Fast-Distrustful Strategy
In the strategy, a requester starts requesting from the root, like the "casual example" above. It requests the values at `0x00` and so on. However, the chain changes over time, and at some point the connected nodes may no longer have the trie data available.

At this point, the requestor needs to "pivot" to a new state root. So the requestor requests the empty prefix at the new state root. However, instead of asking for the `0x00` values *again*, the requestor skips to a prefix that was not retrieved in an earlier trie.

The requestor considers this "stage" of sync complete when keys/values from all prefixes are downloaded, even if they come from different state roots. The follow-up stage is to use "traditional" fast sync to fix up the gaps.

#### Anti-Fast Strategy
A requestor client might choose not to implement fast sync, so they look for an alternative strategy. In this case, every time a pivot is required, the client would have to re-request from the empty prefix. For any key prefix that has not changed since the previous pivot, the client can still skip that whole subtree. Depending on how large the buckets are and how fast state is changing, the client may end up receiving many duplicate keys and values, repeatedly. It will probably involve more requests and more bandwidth consumed, than any other strategy.

#### Fast-Optimistic Strategy
A requestor can choose to ask for prefixes without having the proof from the trie. In this strategy, the nodes aren't any use, so the client would aim to choose a prefix that is long enough to get keys/values. This strategy appears very similar to fast-warp, because you can't verify the chunks along the way. It still has the added bonus of adapting to changing trie size and imbalance. If the requestor picks a prefix that is too short, the responder will reply in a way that indicates that the requester should pick a longer prefix.

#### Fast-Earned-Trust Strategy

This is a combination of Distrustful and Optimistic strategies.

A requester might start up new peers, using the Fast-Distrustful Strategy. After a peer has built up some reputation with valid responses, the requestor could decide to switch them to the Fast-Optimistic strategy. If using multiple peers to sync, a neat side-effect is that the responding peer cannot determine whether or not you have the proof yet, so it's harder for a griefer to grief you opportunistically.

### Response Strategies

#### Request Shaping via Node Response

The responder has more information than the requestor. The responder can communicate the next suggested request by adding (or withholding!) some nodes when responding to a request.

Let's look at an example trie that is imbalanced (binary trie for simplicity): the right-half of a trie is light, but there are a lot of values prefixed with `0b01`.

A request comes in for the empty prefix. A responder who is responding to an empty prefix request can shape the returned nodes to communicate the imbalance.

The responder responds with this scraggly tree:

```
    * (root)
 0 /
  *   
   \ 1
    *

```

The responder has just implied that the requestor's next requests ought to be:

- `0b1`
- `0b00`
- `0b010`
- `0b011`

This strategy isn't only valuable for imbalanced tries. In a well balanced trie, it's still useful to communicate an ideal depth for prefixes with this method. The ideal prefix depth is especially difficult for a requestor to predict in a storage trie, for example.

#### Sending breadth-first nodes

Given that we want to shape requests, but there are more total nodes than we can send in a single response, there is still an open question. Do we prefer depth-first or breadth-first?

It should be easier for the responder and more helpful for the requester to get a breadth of nodes, so that they don't waste time on nodes that are too deep. In other words, it is better to send this:

```
               *

* * * * * * * * * * * * * * * *
```

Than to send this:

```
         *
        /
       *
      /
     *
    /
   *
  /
 *
```

The protocol permits servers to send the latter, but it doesn't seem to be beneficial to anyone. Why? It assumes that the server knows more about which data you want than you do. If the server always prefers low prefixes and you want the 0xFFFF prefix, then it takes many more requests to retrieve, and many more nodes sent. (It takes roughly as many requests as there are nibbles in the prefix of the bucket with your target account hash). Request count for that scenario is cut in ~1/n if breadth-first returns n layers.

#### Value Response

There is only one straightforward strategy: return all the keys and values in the prefix. If you can't return exactly that, then don't return anything. See "Hasty Sync" for an alternative that may be faster, but at the cost of immediate response verifiability.

### Hasty Firehose Sync Variant

One possible variant would take a small tweak: permitting that the server can return leaves that don't *precisely* match the state root requested.

When responding with keys/values, the responder might choose to give results that are nearly valid, but would fail a cryptographic check. Perhaps the values are from one block later, or the responder is reading from a data store that is "mid-block" or "mid-transaction". This enables some caching strategies that leaf sync was designed for.

Naturally, this response strategy comes at the cost of verifiability. For the requestor to validate, they could fast sync against the subtree hash. This fast-verify process might find that 90% of the provided accounts were correct for the given root hash. Perhaps that is acceptable to the requestor, and they choose to keep the responder as a peer.

For performance, requestors wouldn't fast-verify every response. The requesting client can choose how often to validate and how much accuracy to require.

Of course any requesting clients that *don't* use this strategy would treat "Hasty Sync" peers as invalid and stop syncing from them.

This strategy is almost exactly like leaf sync. We don't have an exact spec for leaf sync at the moment, so the comparison is imprecise. The only obvious difference is that account storage is inlined during leaf sync. Firehose still treats storage as separate tries.

## Rejected Variations


### Even-length prefixes

In this variant, odd-length prefixes are not allowed (sometimes saving a byte per requested prefix).

In this scenario, when the responder gives back node data instead of accounts, she would have to always return the node and *all* of its children. Otherwise there would be no way to recurse down into the next layer, because you can't ask for the odd-length prefixed accounts.

The potential value here depends on how meaningful that extra prefix nibble is during requests. Currently, the cost of *requiring* the responder to respond with no nodes or include all children is presumed to be too high.

Additionally, it is convenient for bucket sizes to be able to adjust up and down by 16x instead of 256x.

### Returning leaf nodes instead of key/rlp pairs

In order to rebuild the structure of the trie, we need the full suffix of the key (everything beyond the requested prefix). The leaf node of the trie only includes the suffix of the key from the immediate parent node. Once you include the full suffix, re-including part of the suffix in the leaf node is a waste of bandwidth.


## References

[Yellow Paper] Gavin Wood. Ethereum: A Secure Decentralised Generalised Transaction Ledger, December 2018. [https://ethereum.github.io/yellowpaper](https://ethereum.github.io/yellowpaper).

[eth/63] Vitalik Buterin, Gavin Wood, et al. [Ethereum Wire Protocol (ETH)](https://github.com/ethereum/devp2p/blob/master/caps/eth.md), 2019.

[Size Estimation] Alexey Akhunov. [Estimation (approximate) of the size of contracts in Ethereum](https://medium.com/@akhounov/estimation-approximate-of-the-size-of-contracst-in-ethereum-4642fe92d6fe), April 2019.