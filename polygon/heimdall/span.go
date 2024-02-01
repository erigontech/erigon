package heimdall

import (
	"github.com/google/btree"

	"github.com/ledgerwatch/erigon/polygon/bor/valset"
)

type Span struct {
	Id                SpanId              `json:"span_id" yaml:"span_id"`
	StartBlock        uint64              `json:"start_block" yaml:"start_block"`
	EndBlock          uint64              `json:"end_block" yaml:"end_block"`
	ValidatorSet      valset.ValidatorSet `json:"validator_set,omitempty" yaml:"validator_set"`
	SelectedProducers []valset.Validator  `json:"selected_producers,omitempty" yaml:"selected_producers"`
	ChainID           string              `json:"bor_chain_id,omitempty" yaml:"bor_chain_id"`
}

func (hs *Span) Less(other btree.Item) bool {
	otherHs := other.(*Span)
	if hs.EndBlock == 0 || otherHs.EndBlock == 0 {
		// if endblock is not specified in one of the items, allow search by ID
		return hs.Id < otherHs.Id
	}
	return hs.EndBlock < otherHs.EndBlock
}

func (s *Span) CmpRange(n uint64) int {
	if n < s.StartBlock {
		return -1
	}

	if n > s.EndBlock {
		return 1
	}

	return 0
}

type SpanResponse struct {
	Height string `json:"height"`
	Result Span   `json:"result"`
}
