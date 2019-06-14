# Turbo-Geth programming guide

## Ethereum State
On high level, Ethereum state is a collection of accounts. An account can be either a non-contract (also known as "Externally Owned Account", or EOA),
or smart contract. Type `Account` [core/state/state_object.go](../../core/state/state_object.go) lists the main components of an account's data structure:
1. Nonce
2. Balance
3. Root
4. Code hash

