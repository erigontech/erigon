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

// SpanBlockProducerSelection represents the block producer selection at each epoch
// with their corresponding accumulated ProposerPriority.
//
// In the context of the bor chain, an epoch is equal to 1 span, while
// in the context of the heimdall chain, an epoch is equal to 1 checkpoint.
// This data type aims to make this distinction a bit more visible and is
// intended to be used specifically for span based epochs.
//
// The difference between SpanBlockProducerSelection and Span.SelectedProducers
// is that SpanBlockProducerSelection contains the correct accumulated
// ProposerPriority for each selected producer, while Span.SelectedProducers
// always has ProposerPriority=0.
//
// This is because the heimdall/bor/span/<spanId>
// API only tells us what the "frozen" selected producers for the next span epoch
// are. More info about how that works can be found in the "FreezeSet" logic in
// heimdall at https://github.com/maticnetwork/heimdall/tree/master/bor#how-does-it-work.
//
// However, to correctly calculate the accumulated proposer priorities, one has to start
// from span zero, create a ValidatorSet, call IncrementProposerPriority(spanSprintCount)
// and at every next span call bor.GetUpdatedValidatorSet(oldValidatorSet, span.SelectedProducers)
// and repeat.
type SpanBlockProducerSelection struct {
	SpanId     SpanId
	StartBlock uint64
	EndBlock   uint64
	Producers  *ValidatorSet
}

var _ Entity = (*SpanBlockProducerSelection)(nil)

func (s *SpanBlockProducerSelection) RawId() uint64 {
	return uint64(s.SpanId)
}

func (s *SpanBlockProducerSelection) BlockNumRange() ClosedRange {
	return ClosedRange{
		Start: s.StartBlock,
		End:   s.EndBlock,
	}
}

func (s *SpanBlockProducerSelection) SetRawId(id uint64) {
	s.SpanId = SpanId(id)
}

func (s *SpanBlockProducerSelection) CmpRange(n uint64) int {
	return cmpBlockRange(s.StartBlock, s.EndBlock, n)
}
