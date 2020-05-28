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
	toBlock := block + mgr.BlocksPerCycle + 100
	var prevTick *mgr.Tick

	var sizeFromTicksAccumulator uint64
	for block <= toBlock {
		tick := mgr.NewTick(block, stateSize, prevTick)

		sizeFromTicksAccumulator += tick.ToSize - tick.FromSize
		// props
		if prevTick != nil && prevTick.Number < tick.Number {
			require.LessOrEqual(block, tick.FromBlock)
			require.Less(tick.FromBlock, tick.ToBlock)
			require.Less(prevTick.ToBlock, tick.FromBlock)

			require.LessOrEqual(tick.ToSize, stateSize)
			require.Less(tick.FromSize, tick.ToSize)
			require.Less(prevTick.ToSize, tick.FromSize)
		}

		prevTick = tick
		block = tick.ToBlock + 1
	}
}
