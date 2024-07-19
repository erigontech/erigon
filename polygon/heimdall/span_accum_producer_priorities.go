package heimdall

import "github.com/erigontech/erigon/polygon/bor/valset"

var _ Entity = (*SpanAccumProducerPriorities)(nil)

// SpanAccumProducerPriorities represents the block producer selection at each epoch
// with their corresponding accumulated ProposerPriority.
//
// In the context of the bor chain, an epoch is equal to 1 span, while
// in the context of the heimdall chain, an epoch is equal to 1 checkpoint.
// This data type aims to make this distinction a bit more visible and is
// intended to be used specifically for span based epochs.
//
// The difference between SpanAccumProducerPriorities and Span.SelectedProducers
// is that SpanAccumProducerPriorities contains the correct accumulated
// ProposerPriority for each selected producer, while Span.SelectedProducers
// always has ProposerPriority=0.
//
// This is because the heimdall/bor/span/<spanId>
// API only tells us what the "frozen" selected producers for the next span epoch
// are. More info about how that works can be found in the "FreezeSet" logic in
// heimdall at https://github.com/maticnetwork/heimdall/tree/master/bor#how-does-it-work.
//
// However, to correctly calculate the accumulated proposer priorities, one has to start
// from span zero, create a valset.ValidatorSet, call IncrementProposerPriority(spanSprintCount)
// and at every next span call bor.GetUpdatedValidatorSet(span.SelectedProducers) and repeat.
type SpanAccumProducerPriorities struct {
	SpanId     SpanId
	StartBlock uint64
	EndBlock   uint64
	Producers  []*valset.Validator
}

func (s *SpanAccumProducerPriorities) RawId() uint64 {
	return uint64(s.SpanId)
}

func (s *SpanAccumProducerPriorities) BlockNumRange() ClosedRange {
	return ClosedRange{
		Start: s.StartBlock,
		End:   s.EndBlock,
	}
}

func (s *SpanAccumProducerPriorities) SetRawId(id uint64) {
	s.SpanId = SpanId(id)
}
