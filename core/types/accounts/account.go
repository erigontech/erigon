package accounts

import (
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
)

type ExtAccount struct {
	Nonce   uint64
	Balance *big.Int
}

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
type Account struct {
	Nonce       uint64
	Balance     *big.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    []byte
	StorageSize *uint64
}