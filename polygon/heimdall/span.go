// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package heimdall

import (
	"github.com/google/btree"

	"github.com/erigontech/erigon/polygon/bor/valset"
)

type Span struct {
	Id                SpanId              `json:"span_id" yaml:"span_id"`
	StartBlock        uint64              `json:"start_block" yaml:"start_block"`
	EndBlock          uint64              `json:"end_block" yaml:"end_block"`
	ValidatorSet      valset.ValidatorSet `json:"validator_set,omitempty" yaml:"validator_set"`
	SelectedProducers []valset.Validator  `json:"selected_producers,omitempty" yaml:"selected_producers"`
	ChainID           string              `json:"bor_chain_id,omitempty" yaml:"bor_chain_id"`
}

var _ Entity = &Span{}

func (s *Span) RawId() uint64 {
	return uint64(s.Id)
}

func (s *Span) SetRawId(_ uint64) {
}

func (s *Span) BlockNumRange() ClosedRange {
	return ClosedRange{
		Start: s.StartBlock,
		End:   s.EndBlock,
	}
}

func (s *Span) Less(other btree.Item) bool {
	otherHs := other.(*Span)
	if s.EndBlock == 0 || otherHs.EndBlock == 0 {
		// if endblock is not specified in one of the items, allow search by ID
		return s.Id < otherHs.Id
	}
	return s.EndBlock < otherHs.EndBlock
}

func (s *Span) CmpRange(n uint64) int {
	return cmpBlockRange(s.StartBlock, s.EndBlock, n)
}

func (s *Span) Producers() []*valset.Validator {
	res := make([]*valset.Validator, len(s.SelectedProducers))
	for i, p := range s.SelectedProducers {
		pCopy := p
		res[i] = &pCopy
	}

	return res
}

type SpanResponseV1 struct {
	Height string `json:"height"`
	Result Span   `json:"result"`
}

type SpanResponseV2 struct {
	Span *Span `json:"span"`
}

type spans []*Span

func (s spans) Len() int {
	return len(s)
}

func (s spans) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func (s spans) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SpanListResponse struct {
	Height string `json:"height"`
	Result spans  `json:"result"`
}
