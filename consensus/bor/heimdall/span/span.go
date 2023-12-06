package span

import (
	"github.com/google/btree"

	"github.com/ledgerwatch/erigon/consensus/bor/valset"
)

const (
	ZerothSpanEnd   = 255
	NumBlocksInSpan = 6400
)

// Span represents a current bor span
type Span struct {
	ID         uint64 `json:"span_id" yaml:"span_id"`
	StartBlock uint64 `json:"start_block" yaml:"start_block"`
	EndBlock   uint64 `json:"end_block" yaml:"end_block"`
}

// HeimdallSpan represents span from heimdall APIs
type HeimdallSpan struct {
	Span
	ValidatorSet      valset.ValidatorSet `json:"validator_set" yaml:"validator_set"`
	SelectedProducers []valset.Validator  `json:"selected_producers" yaml:"selected_producers"`
	ChainID           string              `json:"bor_chain_id" yaml:"bor_chain_id"`
}

func (hs *HeimdallSpan) Less(other btree.Item) bool {
	otherHs := other.(*HeimdallSpan)
	if hs.EndBlock == 0 || otherHs.EndBlock == 0 {
		// if endblock is not specified in one of the items, allow search by ID
		return hs.ID < otherHs.ID
	}
	return hs.EndBlock < otherHs.EndBlock
}

// OfBlockNumber returns the span id of block id.
func OfBlockNumber(blockNumber uint64) uint64 {
	var spanNumber uint64
	if blockNumber > ZerothSpanEnd {
		spanNumber = 1 + (blockNumber-ZerothSpanEnd)/NumBlocksInSpan
	}

	return spanNumber
}

// StartBlockNumber returns the start block id for a given span id.
func StartBlockNumber(spanNumber uint64) uint64 {
	if spanNumber == 0 {
		return 1
	}

	return EndBlockNumber(spanNumber-1) + 1
}

// EndBlockNumber returns the end block id for a given span id.
func EndBlockNumber(spanNumber uint64) uint64 {
	if spanNumber == 0 {
		return ZerothSpanEnd
	}

	return ZerothSpanEnd + spanNumber*NumBlocksInSpan
}
