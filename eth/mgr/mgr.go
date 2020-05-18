package mgr

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/state"
)

const (
	TicksPerCycle  uint64 = 256
	BlocksPerTick  uint64 = 20
	BlocksPerCycle uint64 = BlocksPerTick * TicksPerCycle

	BytesPerWitness uint64 = 1024 * 1024
)

type Schedule struct {
	Ticks []Tick
}

type Tick struct {
	Number          uint64
	FromBlock       uint64
	ToBlock         uint64
	FromSize        uint64
	ToSize          uint64
	StateSizeSlices []StateSizeSlice
}

type StateSizeSlice struct {
	FromSize uint64
	ToSize   uint64
}

type StateSlice struct {
	From []byte
	To   []byte
}

func (s Schedule) String() string { return fmt.Sprintf("Schedule{Ticks:%s}", s.Ticks) }
func (t Tick) String() string {
	return fmt.Sprintf("Tick{%d,Blocks:%d-%d,Sizes:%d-%d,Slices:%d}", t.Number, t.FromBlock, t.ToBlock, t.FromSize, t.ToSize, t.StateSizeSlices)
}
func (ss StateSlice) String() string { return fmt.Sprintf("{%x-%x}", ss.From, ss.To) }

func (t Tick) IsLastInCycle() bool {
	return t.Number == TicksPerCycle-1
}

func NewStateSchedule(stateSize, fromBlock, toBlock uint64) Schedule {
	schedule := Schedule{}

	for fromBlock <= toBlock {
		tick := NewTick(fromBlock, stateSize)
		schedule.Ticks = append(schedule.Ticks, tick)
		fromBlock = tick.ToBlock + 1
	}

	return schedule
}

func NewTick(blockNr, stateSize uint64) Tick {
	number := blockNr / BlocksPerTick % TicksPerCycle
	fromSize := number * stateSize / TicksPerCycle

	tick := Tick{
		Number:    number,
		FromBlock: blockNr,
		ToBlock:   blockNr - blockNr%BlocksPerTick + BlocksPerTick - 1,
		FromSize:  fromSize,
		ToSize:    fromSize + stateSize/TicksPerCycle - 1,
	}

	for i := uint64(0); ; i++ {
		ss := StateSizeSlice{
			FromSize: tick.FromSize + i*BytesPerWitness,
			ToSize:   min(tick.FromSize+(i+1)*BytesPerWitness-1, tick.ToSize),
		}

		tick.StateSizeSlices = append(tick.StateSizeSlices, ss)
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

// Temporary unoptimal implementation. Get existing short prefixes from trie, then resolve range, and give long prefixes from trie.
func StateSizeSlice2StateSlice(tds *state.TrieDbState, in StateSizeSlice) (out StateSlice, err error) {
	out.From, out.To, err = tds.PrefixByCumulativeWitnessSize(in.FromSize, in.ToSize)
	return out, err
}
