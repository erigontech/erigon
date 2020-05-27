package mgr

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

const (
	TicksPerCycle  uint64 = 256
	BlocksPerTick  uint64 = 20
	BlocksPerCycle uint64 = BlocksPerTick * TicksPerCycle

	BytesPerWitness uint64 = 1024 * 1024
)

type Tick struct {
	Number    uint64
	FromSize  uint64
	ToSize    uint64
	From      []byte
	To        []byte
	FromBlock uint64
	ToBlock   uint64
}

func (t Tick) String() string {
	return fmt.Sprintf("Tick{%d,Blocks:%d-%d,Sizes:%d-%d,Prefixes:%x-%x}", t.Number, t.FromBlock, t.ToBlock, t.FromSize, t.ToSize, t.From, t.To)
}

func (t Tick) IsLastInCycle() bool {
	return t.Number == TicksPerCycle-1
}

// NewTick constructor building Tick object and calculating all state-size-parameters
// not filling exact keys: from, to
func NewTick(blockNr, stateSize uint64, prevTick *Tick) *Tick {
	number := blockNr / BlocksPerTick % TicksPerCycle
	fromSize := number * stateSize / TicksPerCycle

	tick := &Tick{
		Number:    number,
		FromBlock: blockNr,
		ToBlock:   blockNr - blockNr%BlocksPerTick + BlocksPerTick - 1,
		FromSize:  fromSize,
		ToSize:    fromSize + stateSize/TicksPerCycle - 1,
	}

	if tick.Number != 0 && prevTick != nil {
		prevTick.FromSize = prevTick.ToSize + 1
	}

	return tick
}

type Schedule struct {
	estimator WitnessEstimator
	prevTick  *Tick
}

type WitnessEstimator interface {
	TotalCumulativeWitnessSize() (uint64, error)
	PrefixByCumulativeWitnessSize(from []byte, size uint64) (prefix []byte, err error)

	TotalCumulativeWitnessSizeDeprecated() uint64
	PrefixByCumulativeWitnessSizeDeprecated(size uint64) (prefix []byte, err error)
}

func NewSchedule(estimator WitnessEstimator) *Schedule {
	return &Schedule{estimator: estimator}
}

// Tick - next step of MGR Schedule. Calculating range of keys of valid size
//
// Important: ticks are cycled. When `TicksPerCycle` reached - it starts from beginning of state.
// Means tick.FromSize > prevTick.ToSize - only when tick.Number != 0
func (s *Schedule) Tick(block uint64) (*Tick, error) {
	total, err := s.estimator.TotalCumulativeWitnessSize()
	if err != nil {
		return nil, err
	}

	tick := NewTick(block, total, s.prevTick)
	var prevKey []byte
	if tick.Number != 0 && s.prevTick != nil {
		prevKey = s.prevTick.To
	}
	tick.From, _ = dbutils.NextSubtree(prevKey)
	if tick.To, err = s.estimator.PrefixByCumulativeWitnessSize(tick.From, tick.ToSize-tick.FromSize); err != nil {
		return tick, err
	}
	prevKey = tick.To
	s.prevTick = tick
	return tick, nil
}

func (s *Schedule) TickDeprecated(block uint64) (*Tick, error) {
	tick := NewTick(block, s.estimator.TotalCumulativeWitnessSizeDeprecated(), s.prevTick)
	var err error
	if tick.From, err = s.estimator.PrefixByCumulativeWitnessSizeDeprecated(tick.FromSize); err != nil {
		return tick, err
	}
	if tick.To, err = s.estimator.PrefixByCumulativeWitnessSizeDeprecated(tick.ToSize); err != nil {
		return tick, err
	}

	return tick, nil
}
