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
	FromSize    uint64
	ToSize      uint64
	FromBlock   uint64
	ToBlock     uint64
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
	return fmt.Sprintf("{Sizes:%d-%d,Prefixes:%x-%x}", ss.FromSize, ss.ToSize, ss.From, ss.To)
}

func (t Tick) IsLastInCycle() bool {
	return t.Number == TicksPerCycle-1
}

func newTick(blockNr, stateSize uint64, previousTick *Tick) *Tick {
	number := blockNr / BlocksPerTick % TicksPerCycle
	fromSize := number * stateSize / TicksPerCycle

	tick := &Tick{
		Number:    number,
		FromBlock: blockNr,
		ToBlock:   blockNr - blockNr%BlocksPerTick + BlocksPerTick - 1,
		FromSize:  fromSize,
		ToSize:    fromSize + stateSize/TicksPerCycle - 1,
	}

	for i := uint64(0); ; i++ {
		ss := StateSlice{
			FromSize: fromSize + i*BytesPerWitness,
			ToSize:   min(fromSize+(i+1)*BytesPerWitness-1, tick.ToSize),
		}

		if i == 0 && previousTick != nil {
			ss.From = previousTick.StateSlices[len(previousTick.StateSlices)-1].To
			ss.FromSize = previousTick.StateSlices[len(previousTick.StateSlices)-1].ToSize
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
	lastTick  *Tick
}

type WitnessEstimator interface {
	CumulativeWitnessSize() uint64
	CumulativeWitnessSizeDBOnly() (uint64, error)
	CumulativeWitnessSizeDBOnly2() (uint64, error)
	PrefixByCumulativeWitnessSize(size uint64) (prefix []byte, err error)
	PrefixByCumulativeWitnessSizeDBOnly(size uint64) (prefix []byte, err error)
	PrefixByCumulativeWitnessSizeFrom(from []byte, size uint64) (prefix []byte, err error)
}

func NewSchedule(estimator WitnessEstimator) *Schedule {
	return &Schedule{estimator: estimator}
}

func (s *Schedule) Tick(block uint64) (*Tick, error) {
	tick := newTick(block, s.estimator.CumulativeWitnessSize(), s.lastTick)
	for i := range tick.StateSlices {
		var err error
		if tick.StateSlices[i].From, err = s.estimator.PrefixByCumulativeWitnessSize(tick.StateSlices[i].FromSize); err != nil {
			return tick, err
		}
		if tick.StateSlices[i].To, err = s.estimator.PrefixByCumulativeWitnessSize(tick.StateSlices[i].ToSize); err != nil {
			return tick, err
		}
	}

	s.lastTick = tick
	return tick, nil
}

func (s *Schedule) Tick2(block uint64) (*Tick, error) {
	total, err := s.estimator.CumulativeWitnessSizeDBOnly()
	if err != nil {
		return nil, err
	}

	tick := newTick(block, total, s.lastTick)
	for i := range tick.StateSlices {
		var err error
		if tick.StateSlices[i].From == nil {
			if tick.StateSlices[i].From, err = s.estimator.PrefixByCumulativeWitnessSizeFrom([]byte{}, tick.StateSlices[i].FromSize); err != nil {
				return tick, err
			}
		}
		if tick.StateSlices[i].To, err = s.estimator.PrefixByCumulativeWitnessSizeFrom(tick.StateSlices[i].From, tick.StateSlices[i].ToSize-tick.StateSlices[i].FromSize); err != nil {
			return tick, err
		}
	}

	s.lastTick = tick
	return tick, nil
}

func (s *Schedule) Tick3(block uint64) (*Tick, error) {
	total, err := s.estimator.CumulativeWitnessSizeDBOnly2()
	if err != nil {
		return nil, err
	}

	tick := newTick(block, total, s.lastTick)
	for i := range tick.StateSlices {
		var err error
		if tick.StateSlices[i].From == nil {
			if tick.StateSlices[i].From, err = s.estimator.PrefixByCumulativeWitnessSizeFrom([]byte{}, tick.StateSlices[i].FromSize); err != nil {
				return tick, err
			}
		}
		if tick.StateSlices[i].To, err = s.estimator.PrefixByCumulativeWitnessSizeFrom(tick.StateSlices[i].From, tick.StateSlices[i].ToSize-tick.StateSlices[i].FromSize); err != nil {
			return tick, err
		}
	}

	s.lastTick = tick
	return tick, nil
}
