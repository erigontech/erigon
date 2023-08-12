package bor

import (
	"math/big"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/rlp"
)

//go:generate mockgen -destination=./genesis_contract_mock.go -package=bor . GenesisContract
type GenesisContract interface {
	CommitState(event *clerk.EventRecordWithTime, syscall consensus.SystemCall, i int, newEvent rlp.RawValue) error
	LastStateId(syscall consensus.SystemCall) (*big.Int, error)
}
