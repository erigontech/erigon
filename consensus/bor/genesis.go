package bor

import (
	"math/big"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/statefull"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./genesis_contract_mock.go -package=bor . GenesisContract
type GenesisContract interface {
	CommitState(event *clerk.EventRecordWithTime, state *state.IntraBlockState, header *types.Header, chCtx statefull.ChainContext, syscall consensus.SystemCall) error
	LastStateId(header *types.Header, state *state.IntraBlockState, chain statefull.ChainContext, syscall consensus.SystemCall) (*big.Int, error)
}
