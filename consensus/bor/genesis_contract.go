package bor

import (
	"math/big"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/rlp"
)

//go:generate mockgen -destination=./mock/genesis_contract_mock.go -package=mock . GenesisContract
type GenesisContract interface {
	CommitState(event rlp.RawValue, syscall consensus.SystemCall) error
	LastStateId(syscall consensus.SystemCall) (*big.Int, error)
}
