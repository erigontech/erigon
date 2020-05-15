package mgr_test

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/eth/mgr"
	"github.com/stretchr/testify/require"
)

func TestScheduleProperties(t *testing.T) {
	require := require.New(t)
	stateSize := uint64(123456)
	block := uint64(11)

	var prevTick mgr.Tick
	var prevSlice mgr.StateSizeSlice

	var sizeFromSlicesAccumulator uint64
	var sizeFromTicksAccumulator uint64
	schedule := mgr.NewStateSchedule(stateSize, block, block+mgr.BlocksPerCycle+100)
	for i := range schedule.Ticks {
		tick := schedule.Ticks[i]
		for j := range tick.StateSizeSlices {
			ss := tick.StateSizeSlices[j]
			sizeFromSlicesAccumulator += ss.ToSize - ss.FromSize

			// props
			if prevTick.Number < tick.Number { // because cycles are... cycled
				require.Less(ss.FromSize, ss.ToSize)
				require.LessOrEqual(prevSlice.ToSize, ss.FromSize)
			}

			prevSlice = ss
		}
		sizeFromTicksAccumulator += tick.ToSize - tick.FromSize

		// props
		if prevTick.Number < tick.Number {
			require.LessOrEqual(block, tick.FromBlock)
			require.Less(tick.FromBlock, tick.ToBlock)
			require.Less(prevTick.ToBlock, tick.FromBlock)

			require.LessOrEqual(tick.ToSize, stateSize)
			require.Less(tick.FromSize, tick.ToSize)
			require.Less(prevTick.ToSize, tick.FromSize)
		}

		prevTick = tick
	}

}
