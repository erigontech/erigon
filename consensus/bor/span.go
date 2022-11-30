package bor

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/statefull"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./span_mock.go -package=bor . Spanner
type Spanner interface {
	GetCurrentSpan(header *types.Header, state *state.IntraBlockState, chain statefull.ChainContext, syscall consensus.SystemCall) (*span.Span, error)
	GetCurrentValidators(blockNumber uint64, signer common.Address, getSpanForBlock func(blockNum uint64) (*span.HeimdallSpan, error)) ([]*valset.Validator, error)
	CommitSpan(newSpanID uint64, state *state.IntraBlockState, header *types.Header, chain statefull.ChainContext, heimdallSpan span.HeimdallSpan, syscall consensus.SystemCall) error
}
