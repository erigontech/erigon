Turbo-Geth programmer's guide
==============================

Ethereum State
--------------
On the high level, Ethereum state is a collection of accounts. An account can be either a non-contract
(also known as "Externally Owned Account", or EOA), or a smart contract.

### Content of an account
Type `Account` [core/state/state_object.go](../../core/state/state_object.go) lists the main components
of an account's content (not identifier):

1. Nonce
2. Balance
3. Root
4. Code hash

#### Nonce
Number of the type `uint64`.

For non-contract accounts, nonce is important in two contexts. Firsly, all transactions signed by an account, have to
appear in the ledger in the order of strictly increasing nonces (without gaps). This check is performed by member function `preCheck` of
type `StateTransition` [core/state_transition.go](../../core/state_transition.go)
Secondly, if a transaction signed by an account is sent to no
particular address (with intention of creating a contract), and it ends up creating a new smart contract account, the adress of such newly
created smart contract account is calcuted based on the current nonce of the "creator" account.
For smart contract accounts, nonce is important when `CREATE` opcode is executed on behalf of such account. This computation is
performed in the member function `Create` of the type `EVM` [core/vm/evm.go](../../core/vm/evm.go). Note the difference between the
member function `Create2`, where the address of the newly created contract is independent of the nonce. For contract accounts,
the nonce is important in the context of creating new contracts via `CREATE` opcode.

#### Balance
Number of the type `*big.Int`. Since balance is denominated in wei, and there 10^18 wei in each Ether, maximum balance that needs to be
representable is 110'000'000 (roughly amount of mainnet Ether in existence, but can be more for testnets and private networks),
multiplied by 10^18, which exceeds the capacity of `uint64`.

#### Root
Binary 32-byte (256-bit) string.

By root here one means the Merkle root of the smart contract storage, organised into a tree. Non-contract accounts cannot have storage,
therefore root makes sense only for smart contract accounts. For non-contract accounts, the root field is assumed to be equal to the
Merkle root of empty tree, which is hard-coded in the varible `emptyRoot` in [core/state/database.go](../../core/state/database.go) and
[trie/trie.go](../../trie/trie.go). For contract accounts, the root is computed using member function `Hash` of
type `Trie` [trie/trie.go](../../trie/trie.go), once the storage of the contract has been organised into the tree by calling member functions
`Update` and `Delete` on the same type.

#### Code hash
Binary 32-byte (256-bit) string.

Hash of the bytecode (deployed code) of a smart contract. The computation of the code hash is performed in the `SetCode` member function
of the type `StateDB` [code/state/statedb.go](../../core/state/statedb.go). Since a non-contract account has no bytecode, code hash only
makes sense for smart contract accounts. For non-contract accounts, the code hash is assumed to be equal to the hash of `nil`, which is
hard-coded in the variable `emptyCode` in [code/state/statedb.go](../../core/state/statedb.go)

### Address - identifier of an account
Accounts are identified by their addresses. Address is a 20-byte binary string, which is derived differently for smart contract and
non-contract accounts.

For non-contract accounts, the address is derived from the public key, by hashing it and taking lowest 20 bytes of the 32-byte hash value,
as shown in the function `PubkeyToAddress` in the file [crypto/crypto.go](../../crypto/crypto.go)

For smart contract accounts created by a transaction without destination, or by `CREATE` opcode, the address is derived from the
address and the nonce of the creator , as shown in the function `CreateAddress` in the file [crypto/crypto.go](../../crypto/crypto.go)

For smart contract accounts created by `CREATE2` opcode, the address is derived from the creator's address, salt (256-bit argument
supplied to the `CREATE2` invocation), and the code hash of the initialisation code (code that is executed to output the actual,
deployed code of the new contract), as shown in the function `CreateAddress2` in the file [crypto/crypto.go](../../crypto/crypto.go)

In many places in the code, sets of accounts are represented by mappings from account addresses to the objects representing
the accounts themselves, for example, field `stateObjects` in the type `StateDB` [core/state/statedb.go](../../core/state/statedb.go).
Member functions of the type `StateDB` that are for querying and modifying one of the componets of an accounts, are all accepting
address as their first argument, see functions `GetBalance`, `GetNonce`, `GetCode`, `GetCodeSize`, `GetCodeHash`, `GetState` (this
one queries an item in the contract storage), `GetCommittedState`, `AddBalance`, `SubBalance`, `SetBalance`, `SetNonce`,
`SetCode`, `SetState` (this one modifies an item in the contract storage) [core/state/statedb.go](../../core/state/statedb.go).

Organising Ethereum State into a Merkle Tree
--------------------------------------------
Ethereum network produces checkpoints of the Ethereum State after every block. These checkpoints come in a form of 32-byte binary
string, which is the root hash of the Merkle tree constructed out of the accounts in the state. This root hash is often
referred to as "State root". It is part of block header, and is contained in the field `Root` of the type
`Header` [core/types/block.go](../../core/types/block.go)

Prior to Byzantium release, the state root was also part of every transaction receipt, and was contained in the field `PostState`
of the type `Receipt` [core/types/receipt.go](../../core/types/receipt.go). Side note: that is why the member function
`ComputeTrieRoots` in the type `TrieDbState` [core/state/database.go](../../core/state/database.go) returns a slice of hashes,
rather than just a single hash. For pre-Byzantium blocks, this function computes state roots for each transaction receipt, and
another one for the block header. For post-Byzantium blocks, it always returns a singleton slice.

### Hexary radix "Patricia" tree
Ethereum uses hexary (radix == 16) radix tree to guide the algorithm of computing the state root. For the purposes of
illustrations, we will use tres trees with radix 4 (because radix 16 requires many more items for "interesting" features
to appear). We start from a set of randomly looking keys, 4 bytes (or 16 quarternary digits) each.

![prefix_groups_1](prefix_groups_1.dot.gd.png)
To regenerate this picture, run `go run cmd/pics/pics.go -pic prefix_groups_1`

Next, we sort them in lexicographic order.

![prefix_groups_2](prefix_groups_2.dot.gd.png)
To regenerate this picture, run `go run cmd/pics/pics.go -pic prefix_groups_2`

Next, we introduce the notion of a prefix group. Collection of adjacent keys form a prefix group if these keys share
the same prefix, and no other keys share this prefix. Here are the prefix groups for our example:

![prefix_groups_3](prefix_groups_3.dot.gd.png)
To regenerate this picture, run `go run cmd/pics/pics.go -pic prefix_groups_3`

The entire collection of keys form one implicit prefix group, with the empty prefix.

Merke patricia tree hashing recursively replaces prefix groups with their hashes. 