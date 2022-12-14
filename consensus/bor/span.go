package bor

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
)

//go:generate mockgen -destination=./span_mock.go -package=bor . Spanner
type Spanner interface {
	GetCurrentSpan(syscall consensus.SystemCall) (*span.Span, error)
	GetCurrentValidators(blockNumber uint64, signer common.Address, getSpanForBlock func(blockNum uint64) (*span.HeimdallSpan, error)) ([]*valset.Validator, error)
	CommitSpan(heimdallSpan span.HeimdallSpan, syscall consensus.SystemCall) error
}
