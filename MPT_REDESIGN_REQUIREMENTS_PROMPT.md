Requirements for New MPT implementation

1. easily debuggable
2. easily comparable to other clients (to help with debugging)
3. mutations confined in one place
4. Clear node types: i.e. account leaf node, storage leaf node, extension node, branch node
reduces the number of case variations - e.g. get rid of stuff like is it memoized , get rid of fused cells, etc
5. Clear DB layer separation
6. Trie reader interface to read a trie node at a given path (useful for eth_getProof )
7. Reuse StateReader interface used for account data, with a separate reader for commitment data
8. Accounts should store storage root
9. Visitor pattern should be clear for tracing hooks (useful for debug_executionWitness)
10. Get rid of KeyCommitment state (it tracks the most recent block and txnum for which state root was calculated, and state root). We can get rid of that by storing path->(NodeData,txnum). So SeekCommitment() can use the txnum from the value instead.
11. We don’t need to do batched commitments anymore
snapshoters always compute it once per block to generate commitment history
users never have to exec more than a week (or few weeks) worth of blocks at initial sync (they don’t care about it)
let’s get rid of it - we don’t need to have an “algo” which has to handle millions and millions worth of key updates that “don’t fit in memory” using a conventional approach of building a tree data structure


There should be an in-memory trie that use pointers to expand various branches according to the needs at hand. The schema should be path->(NodeData,txnum). 

NodeData should be as compactly represented as possible. But the reader interface should uncompact it into the business logic level representation, and the encoding layer should compact it when writing to db.

The trie should be able to dynamically fit many expanded trie paths at the same time (e.g. in a concurrent algorithm). The serial algorithm should work by processing keys in lexicographic order which corresponds to an in-order traversal of the MPT.

Other points:
1. Get rid of DuoNode 
2. Since the schema for commitment will be path->nodedata and we don't want to duplicate data from accounts and storage domains, it is important that you consider ways to tackle this problem. For example, instead os storing the account's RLP we can store its address which is a reference to the accounts domain, and the internal implementation details can be hidden by the reader interface.