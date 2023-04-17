package fakevm

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
)

// FakeDB is the interface state access for the FakeEVM
type FakeDB interface {
	SetStateRoot(stateRoot []byte)
	GetBalance(address common.Address) *big.Int
	GetNonce(address common.Address) uint64
	GetCode(address common.Address) []byte
	GetState(address common.Address, hash common.Hash) common.Hash
	Exist(address common.Address) bool
	GetCodeHash(address common.Address) common.Hash
}
