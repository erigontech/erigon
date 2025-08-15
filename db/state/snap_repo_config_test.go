package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapcfg"
)

// 1. safety margin is respected
// 2. minimum size is respected
// 3. merge stages work
// 4. preverifiedparsed is respected

func TestFreezingRangeNoPreverified(t *testing.T) {
	cfg := createConfig(t)

	cases := []struct {
		testName           string
		inputFrom, inputTo uint64
		outFrom, outTo     uint64
		canFreeze          bool
	}{
		{
			"no safety margin1", 0, 999,
			0, 0, false,
		},
		{
			"no safety margin2", 0, 1000,
			0, 0, false,
		},
		{
			"no safety margin3", 0, 1004,
			0, 0, false,
		},
		{
			"min snap", 0, 1005,
			0, 1000, true,
		},
		{
			"min snap2", 0, 2005,
			0, 1000, true,
		},
		{
			"min snap3", 0, 10_000 + 5,
			0, 10_000, true,
		},
		{
			"min snap - from round off", 4, 10_000 + 5,
			0, 10_000, true,
		},
		{
			"biggest merge limit", 0, 100_000 + 5,
			0, 100_000, true,
		},
		{
			"from a different from", 250_000, 350_000 + 5,
			250_000, 260_000, true,
		},
		{
			"from a different from2", 400_000, 500_000 + 5,
			400_000, 500_000, true,
		},
	}

	for _, testCase := range cases {
		from, to, canFreeze := getFreezingRange(RootNum(testCase.inputFrom), RootNum(testCase.inputTo), cfg)
		require.Equal(t, testCase.canFreeze, canFreeze)
		if canFreeze {
			require.Equal(t, testCase.outFrom, uint64(from), testCase.testName)
			require.Equal(t, testCase.outTo, uint64(to), testCase.testName)
		}
	}
}

func TestFreezingRangeWithPreverified(t *testing.T) {
	cfg := createConfig(t)
	cfg.LoadPreverified([]snapcfg.PreverifiedItem{
		{
			Name: "v1.0-000000-000500-bodies.seg",
			Hash: "blahblah",
		},
	})

	cases := []struct {
		testName           string
		inputFrom, inputTo uint64
		outFrom, outTo     uint64
		canFreeze          bool
	}{
		{
			"no safety margin1", 0, 999,
			0, 0, false,
		},
		{
			"no safety margin2", 0, 1000,
			0, 0, false,
		},
		{
			"no safety margin3", 0, 1004,
			0, 0, false,
		},
		{
			"min snap", 0, 1005,
			0, 1000, true,
		},
		{
			"min snap2", 0, 2005,
			0, 1000, true,
		},
		{
			"min snap3", 0, 10_000 + 5,
			0, 10_000, true,
		},
		{
			"min snap - from round off", 4, 10_000 + 5,
			0, 10_000, true,
		},
		{
			"biggest merge limit - cfg", 0, 100_000 + 5,
			0, 100_000, true,
		},
		{
			"biggest merge limit - preverified (just short)", 0, 500_000,
			0, 100_000, true,
		},
		{
			"biggest merge limit - preverified", 0, 500_000 + 5,
			0, 500_000, true,
		},
		{
			"preverified size, but no preverified", 100_000, 600_000 + 5,
			100_000, 200_000, true,
		},
	}

	for _, testCase := range cases {
		from, to, canFreeze := getFreezingRange(RootNum(testCase.inputFrom), RootNum(testCase.inputTo), cfg)
		require.Equal(t, testCase.canFreeze, canFreeze)
		if canFreeze {
			require.Equal(t, testCase.outFrom, uint64(from), testCase.testName)
			require.Equal(t, testCase.outTo, uint64(to), testCase.testName)
		}
	}
}

func createConfig(t *testing.T) *SnapshotConfig {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(1000)

	return NewSnapshotConfig(
		&SnapshotCreationConfig{
			RootNumPerStep: stepSize,
			MergeStages:    []uint64{10 * stepSize, 100 * stepSize},
			MinimumSize:    stepSize,
			SafetyMargin:   5,
		},
		NewE2SnapSchema(dirs, "bodies"),
	)
}
