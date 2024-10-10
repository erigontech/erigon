package span

import (
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
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
