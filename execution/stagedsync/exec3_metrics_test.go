package stagedsync

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestExecutionRepeatStats(t *testing.T) {
	tests := []struct {
		name            string
		execCount       uint64
		prevExecCount   uint64
		repeatCount     uint64
		prevRepeatCount uint64
		wantExecDiff    uint64
		wantRepeats     uint64
		wantRatio       float64
	}{
		{
			name:         "first clean interval",
			execCount:    4,
			wantExecDiff: 4,
		},
		{
			name:          "subsequent interval with one redispatch",
			execCount:     8,
			prevExecCount: 4,
			repeatCount:   1,
			wantExecDiff:  4,
			wantRepeats:   1,
			wantRatio:     25,
		},
		{
			name:            "subsequent clean interval",
			execCount:       10,
			prevExecCount:   8,
			repeatCount:     1,
			prevRepeatCount: 1,
			wantExecDiff:    2,
		},
		{
			name:            "no completed dispatches",
			execCount:       10,
			prevExecCount:   10,
			repeatCount:     1,
			prevRepeatCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execDiff, repeats, ratio := executionRepeatStats(
				tt.execCount,
				tt.prevExecCount,
				tt.repeatCount,
				tt.prevRepeatCount,
			)

			require.Equal(t, tt.wantExecDiff, execDiff)
			require.Equal(t, tt.wantRepeats, repeats)
			require.Equal(t, tt.wantRatio, ratio)
		})
	}
}

func TestRecordBlockExecMetricsCountsRuns(t *testing.T) {
	pe := &parallelExecutor{}
	be := newBlockExec(1, common.Hash{}, nil, nil, discardConsumers(1), false, nil)
	be.runInc = make([]atomic.Int64, 3)
	be.runInc[0].Store(0)
	be.runInc[1].Store(2)
	be.runInc[2].Store(-1)

	pe.recordBlockExecMetrics(be)

	execCount, repeatCount := pe.dispatchCounts.load()
	require.Equal(t, uint64(4), execCount)
	require.Equal(t, uint64(2), repeatCount)
}
