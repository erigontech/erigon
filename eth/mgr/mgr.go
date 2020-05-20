package mgr

import (
	"fmt"
)

const (
	TicksPerCycle  uint64 = 256
	BlocksPerTick  uint64 = 20
	BlocksPerCycle uint64 = BlocksPerTick * TicksPerCycle

	BytesPerWitness uint64 = 1024 * 1024
)

type Tick struct {
	Number      uint64
	FromBlock   uint64
	ToBlock     uint64
	FromSize    uint64
	ToSize      uint64
	StateSlices []StateSlice
}

type StateSlice struct {
	FromSize uint64
	ToSize   uint64
	From     []byte
	To       []byte
}

func (t Tick) String() string {
	return fmt.Sprintf("Tick{%d,Blocks:%d-%d,Sizes:%d-%d,Slices:%s}", t.Number, t.FromBlock, t.ToBlock, t.FromSize, t.ToSize, t.StateSlices)
}
func (ss StateSlice) String() string {
	return fmt.Sprintf("{Sizes:%x-%x,Prefixes:%x-%x}", ss.FromSize, ss.ToSize, ss.From, ss.To)
}

func (t Tick) IsLastInCycle() bool {
	return t.Number == TicksPerCycle-1
}

func newTick(blockNr, stateSize uint64) Tick {
	number := blockNr / BlocksPerTick % TicksPerCycle
	fromSize := number * stateSize / TicksPerCycle
	fmt.Printf("%d %d,%d\n", blockNr, blockNr-blockNr%BlocksPerTick+BlocksPerTick-1, blockNr%BlocksPerTick)

	tick := Tick{
		Number:    number,
		FromBlock: blockNr,
		ToBlock:   blockNr - blockNr%BlocksPerTick + BlocksPerTick - 1,
		FromSize:  fromSize,
		ToSize:    fromSize + stateSize/TicksPerCycle - 1,
	}

	for i := uint64(0); ; i++ {
		ss := StateSlice{
			FromSize: tick.FromSize + i*BytesPerWitness,
			ToSize:   min(tick.FromSize+(i+1)*BytesPerWitness-1, tick.ToSize),
		}

		tick.StateSlices = append(tick.StateSlices, ss)
		if ss.ToSize >= tick.ToSize {
			break
		}
	}

	return tick
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

type Schedule struct {
	estimator WitnessEstimator
}

type WitnessEstimator interface {
	CumulativeWitnessLen(key []byte) uint64
	PrefixByCumulativeWitnessLen(size uint64) (prefix []byte, err error)
}

func NewSchedule(estimator WitnessEstimator) *Schedule {
	return &Schedule{estimator: estimator}
}

func (s *Schedule) Tick(block uint64) (Tick, error) {
	tick := newTick(block, s.estimator.CumulativeWitnessLen([]byte{}))
	tick.StateSlices = append(tick.StateSlices)
	for i := range tick.StateSlices {
		var err error
		if tick.StateSlices[i].From, err = s.estimator.PrefixByCumulativeWitnessLen(tick.StateSlices[i].FromSize); err != nil {
			return Tick{}, err
		}
		if tick.StateSlices[i].To, err = s.estimator.PrefixByCumulativeWitnessLen(tick.StateSlices[i].ToSize); err != nil {
			return Tick{}, err
		}
	}

	return tick, nil

}
