# Turbo-Geth programming guide

## Ethereum State
On the high level, Ethereum state is a collection of accounts. An account can be either a non-contract
(also known as "Externally Owned Account", or EOA), or a smart contract.
Type `Account` [core/state/state_object.go](../../core/state/state_object.go) lists the main components
of an account's data structure:

1. Nonce
2. Balance
3. Root
4. Code hash

### Nonce
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

### Balance
Number of the type `*big.Int`. Since balance is denominated in wei, and there 10^18 wei in each Ether, maximum balance that needs to be
representable is 110'000'000 (roughly amount of mainnet Ether in existence, but can be more for testnets and private networks),
multiplied by 10^18, which exceeds the capacity of `uint64`.

### Root
Binary 32-byte (256-bit) string.

By root here one means the Merkle root of the smart contract storage, organised into a tree. Non-contract accounts cannot have storage,
therefore root makes sense only for smart contract accounts. For non-contract accounts, the root field is assumed to be equal to the
Merkle root of empty tree, which is hard-coded in the varible `emptyRoot` in [core/state/database.go](../../core/state/database.go) and
[trie/trie.go](../../trie/trie.go). For contract accounts, the root is computed using member function `Hash` of
type `Trie` [trie/trie.go](../../trie/trie.go), once the storage of the contract has been organised into the tree by calling member functions
`Update` and `Delete` on the same type.

### Code hash
Binary 32-byte (256-bit) string.

Hash of the bytecode (deployed code) of a smart contract. The computation of the code hash is performed in the `SetCode` member function
of the type `StateDB` [code/state/statedb.go](../../core/state/statedb.go). Since a non-contract account has no bytecode, code hash only
makes sense for smart contract accounts. For non-contract accounts, the code hash is assumed to be equal to the hash of `nil`, which is
hard-coded in the variable `emptyCode` in [code/state/statedb.go](../../core/state/statedb.go)
