package bor

import (
	"math/big"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/rlp"
)

//go:generate mockgen -destination=./genesis_contract_mock.go -package=bor . GenesisContract
type GenesisContract interface {
	CommitState(event rlp.RawValue, syscall consensus.SystemCall) error
	LastStateId(syscall consensus.SystemCall) (*big.Int, error)
}
