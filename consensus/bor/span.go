package bor

import (
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	libcommon "github.com/ledgerwatch/erigon/erigon-lib/common"
)

//go:generate mockgen -destination=./span_mock.go -package=bor . Spanner
type Spanner interface {
	GetCurrentSpan(syscall consensus.SystemCall) (*span.Span, error)
	GetCurrentValidators(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error)
	GetCurrentProducers(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error)
	CommitSpan(heimdallSpan span.HeimdallSpan, syscall consensus.SystemCall) error
}
