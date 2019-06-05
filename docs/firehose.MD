# Firehose Sync v0.2

:::info
See [Firehose Sync v0.1](/eXnqtO_vQquzrFDPHjuaFQ) for goals, background, and summaries of similar proposals.
:::

Some goals of the change:
- Enable responses from a different state root hash
- Return a hint of available state roots, if the client asks for an unavailable root
- Split `GetNodeData` into `GetBytecode`, `GetStateNodeData`, and `GetStorageNodeData` per [this request](https://ethereum-magicians.org/t/forming-a-ring-eth-v64-wire-protocol-ring/2857/10?u=carver)
- Include Account hash with request for storage data, to speed up duplicated storage lookups
- Move prefix-length disambiguating data to end of key prefix

## Casual Examples of Firehose Sync

The following aimes to give intuition of different options for how you might use Firehose, but skips the gritty details. Navigate down to [Commands Spec](#Command-Specification-for-Firehose-Sync) if you hate intuition.

In the following examples, `client` means the node that is trying to sync up, and `server` is the node that is providing the sync data to the `client`.

### Beam Sync

In this example, a client would like to run as a full node and execute transactions in a block, but has only downloaded the headers so far. It starts executing transactions, retrieving any missing state on-demand.

1. Extract the sender of the 1st transaction, check for available balance
3. There is no local state, so the storage errors out
4. Request the chunk of accounts near the sender, and re-execute the transaction
5. Look up the account of the receipient, it was not in the chunk from 3, so it errors
6. Request the chunk of accounts near the recipient, and re-execute the transaction
8. Repeat for every transaction, including EVM execution. Pause & request every time the EVM is missing data.
9. Keep all requested and written data, to request less and less data in each block
10. If blocks are executing without fault, but trie is incomplete, request data in background

This has the significant benefit that the client is asking the server for data that is very likely to be hot in cache. (Since the server itself had to recently load the data to execute the blocks)

Drilling in: what does it look like to request a chunk of addresses near the one you are interested in?

### Requesting Account Data

In Firehose Sync, the client asks for the full set of values at a key prefix. If the set is too large, the server provides proofs for smaller buckets as a hint for which prefixes to ask for next. 

Instead of requesting a particular account, you always request buckets, so you end up with nearby results for each request.

That looks something like:

1) **Client** Give me recent addresses and RLPs of every account at state root `0xa1b2`...
2) **Server** Nope, that is way too many. Here is the root node and all of its children.
3) **Client** Looks like those nodes match the root hash :+1:, how about accounts with address hashes starting with `0x00`, also at state root `0xa1b2`?
5) **Server** Nope, still way too big. Here is the node at prefix `0x00`, and all its children
6) **Client** Great, can I have all the accounts whose hashes start with prefix `0x0000`?
8) **Server** No problem! Here are all the account RLPs whose address hashes start with `0x0000`, and the last 30 bytes of the hash of their addresses
9) **Client** Hooray! I can validate all the accounts by building them into a subtrie and comparing it to a hash received in #4

The protocol is almost exactly the same on storage tries, but requests include the account hash that owns the storage root.

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

:::warning
TODO: drop "fuzzy" flag
:::

### `GetStateData` Request

The requester asks for data defined by a given state root hash and key prefix. This command exclusively deals with retrieving the account RLPs.

The requests can be batched so that multiple prefixes and multiple root hashes may be requested.

:::warning
TODO: limit to single state root
:::

```
{
    # A request id is supplied, to match against the response
    "id": int,
    
    # Whether the client accepts data from a different state root than requested
    "fuzzy": boolean,
    
    # Requests are batched in a list under the single key: prefixes
    "prefixes": [
    
        # Each list item is a two-element tuple
        (
            # The first element is the root hash being requested
            root_hash,
        
            # The second element is a list of prefixes being requested
            [key_prefix, ...]
        ),
        ...
    ]
}
```

#### Key Prefixes Note

A naive encoding of key prefixes would be ambiguous between an even-length prefix starting with the `0x0` nibble and an odd-length prefix that gets right-padded with `0x0` to the next full byte.

So an `0x1` nibble is appended to the end of each odd-length key prefix to disambiguate. An `0x00` byte is appended to the end of each even-length key prefix. The prefix can be empty, in which case no nibble is added.

Some valid prefixes encodings:

- `0x` -- The empty (null) prefix means a request for all values in the trie
- `0x01` -- The odd prefix `0x0`
- `0xF1` -- The odd prefix `0xF`
- `0x0000` -- The even prefix `0x00`
- `0xFF00` -- The even prefix `0xFF`
- `0x0001` -- The odd prefix `0x000`
- `0xFFF1` -- The odd prefix `0xFFF`

Some invalid prefix encodings:
- `0x10` -- Must end with `0x00` or `0x1`
- `0xF0` -- Also not marked as odd or even

*This encoding works, but feels naive. Is there anything better out there?*


#### Example
Say you want the accounts that (after hashing) have the prefixes `0xbeef` and `0x0ddba11`, at state root 0xaaa...aaa. Additionally, you want all the values in the state trie with root 0xbbb...bbb. Your request would look like:
```
{
    "id": 1,
    
    # This client would rather get no result than a result at a different root
    "fuzzy": 0,
    
    "prefixes": [
        (
            # state root hash
            0xaaa...aaa,
            
            # even-length 0xbeef and odd-length 0x0ddba11, encoded
            [0xbeef00, 0x0ddba111]
        ),
        (
            # second state root hash
            0xbbb...bbb,
            
            # ask for all values by requesting the empty prefix
            [0x]
        )
    ]
}
```


### `StateData` Response


The responder has the option of returning either:
1) Some nodes in the subtrie starting at the requested prefix (with some flexibility on which nodes to return), OR
2) **All** the keys & values that are stored in that key prefix range



```
{
    # The ID to be used by the requestor to match the response
    "id": int,
    
    # The response data, in the same ordering of the request
    "data": [

        # there is one element in this outer list for each state root requested
        [
        
            # There is one 2-tuple for every prefix requested
            (
            
                # The responder may choose to reply with a list of trie nodes
                [node, ...],
                
                # OR, the responder may reply with the keys & values
                [
                    # A single key/value pair
                    (
                        # The prefix is omitted from the key
                        key_suffix,
                        
                        # The RLP-encoded value stored in the trie
                        value
                    ),
                    ...
                ]
            ),
            ...
        ],
        ...
    ],

    # If an old state root was requested, return the available roots.
    # Otherwise, return an empty list.
    "available_roots": [
    
        # include an entry for every state root hash that the server supports
        state_root_hash,
        ...
    ]
}
```
<!--
-->

The response must reuse the same ordering of root hashes and prefixes as used for the request. For each prefix, the responder chooses to send exactly one of: node data, or key/value data, or neither.

If the suffix is odd-length (in nibbles), then left-pad with 0's to the nearest full byte. That means that a suffix length-4 of `0x0987` and suffix length-3 `0x987` are sent as the same bytes in a response. It's up to the requestor to recall the prefix length, so they can infer the suffix length.

#### Response Constraints

Some constraints on the response:
- For responses with nodes
    1) Response **must not** return any child nodes without also returning their parents. For example, if exactly one node is returned to a request for the empty prefix, it must be the root node.
    2) Response **must** return parent nodes before any child nodes. Child nodes may be in any order.
- For responses with keys & values
    1) Response **must** include all the keys and values in the prefix, such that rebuilding the sub-trie produces the hash at the requested state root

#### Example
A response to the example request above might look like:


```
{
    "id": 1,
    
    # This is a list of two elements, matching the `prefixes` request example
    "data": [
    
        # All values in this first list correspond to
        # state root hash: 0xaaa...aaa
        [
            # This 2-tuple is for the 0xbeef prefix
            (
                # Node data is not empty: the prefix was too short
                [0xabc..., 0xdef...],
                
                # The second element must be empty if the first is not
                []
            ),
            
            # This 2-tuple is for the 0x0ddba11 prefix
            (
                # Node data is empty: the prefix was long enough to get values
                [],
                
                # This is the list of the key suffixes and values under 0x0ddba11
                [
                    # This is a single key suffix and value pair
                    (0x09876..., 0x111...),
                    
                    # Here, all keys start with 0x0, because they are odd-length
                    
                    # Each key suffix must be 57 nibbles long, which
                    # rounds out to 29 bytes
                    (0x0a1b..., 0x1a2...),
                ]),
        ],
        
        # All values in this second list correspond to
        # state root hash: 0xbbb...bbb
        [
            # The first (only) element in this list is for the empty prefix
            
            # The responder returned nothing, perhaps it hit an internal timeout.
            # An empty tuple is a valid response, as is ([], [])
            (),
        ]
    ],
    
    # this is empty, because the server had data for the requested state root
    "available_roots": []
}
```

### `GetStorageData` Request

Very similar to `GetStateData`: request storage by storage root hash, and key prefix. Also include account hash for clients that find this helpful for indexing. This command exclusively deals with retrieving the storage values.

The requests can be batched so that multiple prefixes and multiple root hashes may be requested.

:::warning
TODO: add single account state root
:::

```
{
    # A request id is supplied, to match against the response
    "id": int,
    
    # Whether the client accepts data from a different state root than requested
    "fuzzy": boolean,
    
    # Requests are batched in a list under the single key: prefixes
    "prefixes": [
    
        # Each list item is a three-element tuple
        (
            # The first element is the hash of the account address
            address_hash,
            
            # The second element is the hash of the requested storage trie root
            storage_root_hash,
            
            # The third element is a list of prefixes being requested
            [key_prefix, ...]
        ),
        ...
    ]
}
```


#### Example
Say you want the storage keys that (after hashing) have the prefixes `0xbeef` and `0x0ddba11`, at storage root 0xbbb...bbb (in hashed account address 0xaaa...aaa). Additionally, you want all the values in the storage trie with root 0xddd...ddd (in hashed account address 0xccc...ccc). Your request would look like:
```
{
    "id": 1,
    
    # This client would rather get no result than a result at a different root
    "fuzzy": 0,
    
    "prefixes": [
        (
            # account address hash
            0xaaa...aaa,
            
            # storage root hash
            0xbbb...bbb,
            
            # even-length 0xbeef and odd-length 0x0ddba11, encoded
            [0xbeef00, 0x0ddba111]
        ),
        (
            # second account address hash
            0xccc...ccc,
            
            # second storage root hash
            0xddd...ddd,
            
            # ask for all values by requesting the empty prefix
            [0x]
        )
    ]
}
```

Note that the syncing client can (and probably should!) skip over storage tries with duplicate roots. There is no need to request an identical storage root twice for two different accounts. This could save you about [480MB of downloads](https://notes.ethereum.org/c/r1ESS_oLE/%2FGCePmXL9TZGzZ_Cm26qxAQ) in the current mainnet.


### `StorageData` Response


The responder has the option of returning either:
1) Some nodes in the subtrie starting at the requested prefix (with some flexibility on which nodes to return), OR
2) **All** the keys & values that are stored in that key prefix range

This looks just like the `StateData` response

```
{
    # The ID to be used by the requestor to match the response
    "id": int,
    
    # The response data, in the same ordering of the request
    "data": [

        # there is one element in this outer list for each state root requested
        [
        
            # There is one 2-tuple for every prefix requested
            (
            
                # The responder may choose to reply with a list of trie nodes
                [node, ...],
                
                # OR, the responder may reply with the keys & values
                [
                    # A single key/value pair
                    (
                        # The prefix is omitted from the key
                        key_suffix,
                        
                        # The RLP-encoded value stored in the trie
                        value
                    ),
                    ...
                ]
            ),
            ...
        ],
        ...
    ],

    # If an old state root was requested, return the available roots.
    # Otherwise, return an empty list.
    "available_roots": [
    
        # include an entry for every state root hash that the server supports
        state_root_hash,
        ...
    ]
}
```

#### Example
A response to the example request above might look like:


```
{
    "id": 1,
    
    # This is a list of two elements, matching the `prefixes` request example
    "data": [
    
        # All values in this first list correspond to
        # storage root hash: 0xbbb...bbb
        [
            # This 2-tuple is for the 0xbeef prefix
            (
                # Node data is not empty: the prefix was too short
                [0xabc..., 0xdef...],
                
                # The second element must be empty if the first is not
                []
            ),
            
            # This 2-tuple is for the 0x0ddba11 prefix
            (
                # Node data is empty: the prefix was long enough to get values
                [],
                
                # This is the list of the key suffixes and values under 0x0ddba11
                [
                    # This is a single key suffix and value pair
                    (0x09876..., 0x111...),
                    
                    # Here, all keys start with 0x0, because they are odd-length
                    
                    # Each key suffix must be 57 nibbles long, which
                    # rounds out to 29 bytes
                    (0x0a1b..., 0x1a2...),
                ]),
        ],
        
        # All values in this second list correspond to
        # storage root hash: 0xddd...ddd
        [
            # The first (only) element in this list is for the empty prefix
            
            # The responder returned nothing, perhaps it hit an internal timeout.
            # An empty tuple is a valid response, as is ([], [])
            (),
        ]
    ],
    
    # this is empty, because the server had data for the requested state root
    "available_roots": []
}
```


:::info
Note that the request is keyed on storage root hashes, but if `available_roots` is returned, they are still for **state roots**, not storage root hashes for the requested account.
:::

### `GetStateNodeData` Request


Just like `GetStateData`, except the response will be constrained to:
1. Return at most one node per requested prefix
2. Never returns buckets of values


```
{
    # A request id is supplied, to match against the response
    "id": int,
    
    # Requests are batched in a list under the single key: prefixes
    "prefixes": [
    
        # Each list item is a two-element tuple
        (
            # The first element is the root hash being requested
            root_hash,
        
            # The second element is a list of prefixes being requested
            [key_prefix, ...]
        ),
        ...
    ]
}
```

### `StateNodeData` Response


```
{
    # The ID to be used by the requestor to match the response
    "id": int,
    
    # The response data, in the same ordering of the request
    "data": [

        # there is one inner list for each state root requested
        [
        
            # A node is returned for each requested key prefix
            node,
            
            ...
        ],
        ...
    ],

    # If an old state root was requested, return the available roots.
    # Otherwise, return an empty list.
    "available_roots": [
    
        # include an entry for every state root hash that the server supports
        state_root_hash,
        ...
    ]
}
```

Node position in the response list must correspond to the prefix position in the request list. Empty node responses are permitted if the server declines to include it. The list may be short or empty, if any trailing nodes are omitted.


### `GetStorageNodeData` Request


Just like `GetStorageData`, except the response will be constrained to:
1. Return at most one node per requested prefix
2. Never returns buckets of values

```
{
    # A request id is supplied, to match against the response
    "id": int,
    
    # Requests are batched in a list under the single key: prefixes
    "prefixes": [
    
        # Each list item is a three-element tuple
        (
            # The first element is the hash of the account address
            address_hash,
            
            # The second element is the hash of the requested storage trie root
            storage_root_hash,
            
            # The third element is a list of prefixes being requested
            [key_prefix, ...]
        ),
        ...
    ]
}
```

### `StorageNodeData` Response


```
{
    # The ID to be used by the requestor to match the response
    "id": int,
    
    # The response data, in the same ordering of the request
    "data": [

        # there is one inner list for each storage root requested
        [
        
            # A node is returned for each requested key prefix
            node,
            
            ...
        ],
        ...
    ],

    # If an old state root was requested, return the available roots.
    # Otherwise, return an empty list.
    "available_roots": [
    
        # include an entry for every state root hash that the server supports
        state_root_hash,
        ...
    ]
}
```

Node position in the response list must correspond to the prefix position in the request list. Empty node responses are permitted if the server declines to include it. The list may be short or empty, if any trailing nodes are omitted.

:::info
Note that the request is for storage root hashes, but if `available_roots` is returned, they are still for **state roots**, not storage root hashes for the requested account.
:::

### `GetBytecode` Request


Just like `GetNodeData`, except:
1. Includes a request ID
2. Will only return bytecode with the corresponding hash, not arbitrary nodes data



```
{
    # The ID to be used by the requestor to match the response
    "id": int,
    
    # Requesting bytecode with the following hashes
    "bytecode_hashes": [
    
        # 32-byte hash of requested bytecode
        bytecode_hash,
        
        ...
    ]
```

### `Bytecode` Response


Just like `NodeData`, except:
1. Includes a request ID
2. Will only return bytecode with the corresponding hash, not arbitrary nodes data



```
{
    # The ID to be used by the requestor to match the response
    "id": int,
    
    # Hashes of bytecode that are being requested
    "bytecodes": [
    
        # arbitrary byte length bytecode
        bytecode,
        
        ...
    ]
```

Bytecode position in the response list must correspond to the position in the request list. Empty bytecode responses are permitted if the server declines to include it. The list may be short or empty, if any trailing bytecodes are omitted.

### Handshakes

This proposal doesn't have a position on the *format* of the handshake, or for headers of requests. If all goes well, these things will be improved during v64.

However, it's likely that a few items will be very valuable to negotiate during handshake. For example:

1) Minimum bucket size that the server will support
2) Maximum bucket size that the client will accept
3) Number of trailing tries that server will maintain

Actually implementing those things is not a hard requirement of Firehose, but would certainly be a nice extension. It is left for future work, when the handshake & header proposals are further along.

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
