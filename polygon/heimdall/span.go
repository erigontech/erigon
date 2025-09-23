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
	"strconv"

	"github.com/google/btree"

	"github.com/erigontech/erigon-lib/common"
)

type SpanId uint64

type Span struct {
	Id                SpanId       `json:"span_id" yaml:"span_id"`
	StartBlock        uint64       `json:"start_block" yaml:"start_block"`
	EndBlock          uint64       `json:"end_block" yaml:"end_block"`
	ValidatorSet      ValidatorSet `json:"validator_set,omitempty" yaml:"validator_set"`
	SelectedProducers []Validator  `json:"selected_producers,omitempty" yaml:"selected_producers"`
	ChainID           string       `json:"bor_chain_id,omitempty" yaml:"bor_chain_id"`
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

func (s *Span) Producers() []*Validator {
	res := make([]*Validator, len(s.SelectedProducers))
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

type validator struct {
	ValID            string `json:"val_id"`
	Address          string `json:"signer"`
	VotingPower      string `json:"voting_power"`
	ProposerPriority string `json:"proposer_priority"`
}

func (v *validator) toValidator() (Validator, error) {
	id, err := strconv.Atoi(v.ValID)
	if err != nil {
		return Validator{}, err
	}

	votingPower, err := strconv.Atoi(v.VotingPower)
	if err != nil {
		return Validator{}, err
	}

	proposerPriority, err := strconv.Atoi(v.ProposerPriority)
	if err != nil {
		return Validator{}, err
	}

	rr := Validator{
		ID:               uint64(id),
		Address:          common.HexToAddress(v.Address),
		VotingPower:      int64(votingPower),
		ProposerPriority: int64(proposerPriority),
	}

	return rr, nil
}

type SpanResponseV2 struct {
	Span struct {
		ID           string `json:"id"`
		StartBlock   string `json:"start_block"`
		EndBlock     string `json:"end_block"`
		ValidatorSet struct {
			Validators []validator `json:"validators"`
			Proposer   validator   `json:"proposer"`
		} `json:"validator_set"`
		SelectedProducers []validator `json:"selected_producers"`
		BorChainID        string      `json:"bor_chain_id"`
	} `json:"span"`
}

func (r *SpanResponseV2) ToSpan() (*Span, error) {
	id, err := strconv.Atoi(r.Span.ID)
	if err != nil {
		return nil, err
	}

	startBlock, err := strconv.Atoi(r.Span.StartBlock)
	if err != nil {
		return nil, err
	}

	endBlock, err := strconv.Atoi(r.Span.EndBlock)
	if err != nil {
		return nil, err
	}

	proposer, err := r.Span.ValidatorSet.Proposer.toValidator()
	if err != nil {
		return nil, err
	}

	s := &Span{
		Id:         SpanId(id),
		StartBlock: uint64(startBlock),
		EndBlock:   uint64(endBlock),
		ValidatorSet: ValidatorSet{
			Validators: make([]*Validator, 0, len(r.Span.ValidatorSet.Validators)),
			Proposer:   &proposer,
		},
		SelectedProducers: make([]Validator, 0, len(r.Span.SelectedProducers)),
		ChainID:           r.Span.BorChainID,
	}

	for i := range r.Span.ValidatorSet.Validators {
		toAppend, err := r.Span.ValidatorSet.Validators[i].toValidator()
		if err != nil {
			return nil, err
		}

		s.ValidatorSet.Validators = append(s.ValidatorSet.Validators, &toAppend)
	}

	for i := range r.Span.SelectedProducers {
		toAppend, err := r.Span.SelectedProducers[i].toValidator()
		if err != nil {
			return nil, err
		}

		s.SelectedProducers = append(s.SelectedProducers, toAppend)
	}

	return s, nil
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

type SpanListResponseV1 struct {
	Height string `json:"height"`
	Result spans  `json:"result"`
}

type SpanListResponseV2 struct {
	SpanList []struct {
		ID           string `json:"id"`
		StartBlock   string `json:"start_block"`
		EndBlock     string `json:"end_block"`
		ValidatorSet struct {
			Validators []validator `json:"validators"`
			Proposer   validator   `json:"proposer"`
		} `json:"validator_set"`
		SelectedProducers []validator `json:"selected_producers"`
		BorChainID        string      `json:"bor_chain_id"`
	} `json:"span_list"`
}

func (v *SpanListResponseV2) ToList() ([]*Span, error) {
	spans := make([]*Span, 0, len(v.SpanList))

	for i := range v.SpanList {
		id, err := strconv.Atoi(v.SpanList[i].ID)
		if err != nil {
			return nil, err
		}

		startBlock, err := strconv.Atoi(v.SpanList[i].StartBlock)
		if err != nil {
			return nil, err
		}

		endBlock, err := strconv.Atoi(v.SpanList[i].EndBlock)
		if err != nil {
			return nil, err
		}

		proposer, err := v.SpanList[i].ValidatorSet.Proposer.toValidator()
		if err != nil {
			return nil, err
		}

		s := &Span{
			Id:         SpanId(id),
			StartBlock: uint64(startBlock),
			EndBlock:   uint64(endBlock),
			ValidatorSet: ValidatorSet{
				Validators: make([]*Validator, 0, len(v.SpanList[i].ValidatorSet.Validators)),
				Proposer:   &proposer,
			},
			SelectedProducers: make([]Validator, 0, len(v.SpanList[i].SelectedProducers)),
			ChainID:           v.SpanList[i].BorChainID,
		}

		for j := range v.SpanList[i].ValidatorSet.Validators {
			toAppend, err := v.SpanList[i].ValidatorSet.Validators[j].toValidator()
			if err != nil {
				return nil, err
			}

			s.ValidatorSet.Validators = append(s.ValidatorSet.Validators, &toAppend)
		}

		for j := range v.SpanList[i].SelectedProducers {
			toAppend, err := v.SpanList[i].SelectedProducers[j].toValidator()
			if err != nil {
				return nil, err
			}

			s.SelectedProducers = append(s.SelectedProducers, toAppend)
		}

		spans = append(spans, s)
	}

	return spans, nil
}
