package core

import (
	"strconv"
	"testing"
)

func TestCalculateNumOfPrunedBlocks(t *testing.T) {
	testcases := []struct {
		CurrentBlock        uint64
		LastPrunedBlock     uint64
		BlocksBeforePruning uint64
		BlocksBatch         uint64

		From        uint64
		To          uint64
		Result      bool
		Description string
	}{
		{

			0,
			0,
			10,
			2,
			0,
			0,
			false,
			"It checks start pruning without sync",
		},
		{
			0,
			0,
			10,
			500,
			0,
			0,
			false,
			"It checks overflow when BlocksBatch>BlocksBeforePruning without sync",
		},
		{
			10,
			5,
			5,
			2,
			5,
			5,
			false,
			"It checks that LastPrunedBlock and BlocksBeforePruning works well",
		},
		{
			10,
			7,
			5,
			2,
			7,
			7,
			false,
			"It checks that LastPrunedBlock and BlocksBeforePruning works well in incorrect state",
		},
		{
			10,
			8,
			5,
			3,
			8,
			8,
			false,
			"It checks that LastPrunedBlock and BlocksBeforePruning works well in incorrect state",
		},
		{
			10,
			0,
			5,
			2,
			0,
			2,
			true,
			"It checks success case",
		},
		{
			10,
			7,
			1,
			2,
			7,
			9,
			true,
			"It checks success case after sync",
		},
		{
			30,
			20,
			1,
			10,
			20,
			29,
			true,
			"It checks success case after sync",
		},
		{
			25,
			20,
			1,
			10,
			20,
			24,
			true,
			"It checks that diff calculates correctly",
		},
	}

	for i := range testcases {
		i := i
		v := testcases[i]
		t.Run("case "+strconv.Itoa(i)+" "+v.Description, func(t *testing.T) {
			from, to, res := calculateNumOfPrunedBlocks(v.CurrentBlock, v.LastPrunedBlock, v.BlocksBeforePruning, v.BlocksBatch)
			if from != v.From || to != v.To || res != v.Result {
				t.Log("res", res, "from", from, "to", to)
				t.Fatal("Failed case", i)
			}
		})
	}
}
