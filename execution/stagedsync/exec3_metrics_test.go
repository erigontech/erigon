package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/exec"
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

func TestScheduleExecutionRepeatCountAcrossPartialBlockResume(t *testing.T) {
	queue := exec.NewQueueWithRetry(2)
	t.Cleanup(queue.Release)

	pe := &parallelExecutor{in: queue}
	be := newBlockExec(7, [32]byte{}, nil, nil, nil, nil, false, nil)
	be.tasks = []*execTask{{
		Task: &exec.TxTask{
			TxNum:   50,
			TxIndex: 7,
		},
		index: 0,
	}}
	be.txIncarnations = []int{0}
	be.execTasks.pushPending(0)

	be.scheduleExecution(t.Context(), pe)

	require.Equal(t, 1, be.cntExec)
	require.Zero(t, be.cntRepeat)
	first, ok := queue.Next(t.Context())
	require.True(t, ok)
	require.Zero(t, first.Version().Incarnation)

	be.execTasks.clearInProgress(0)
	be.execTasks.pushPending(0)
	be.txIncarnations[0] = 1

	be.scheduleExecution(t.Context(), pe)

	require.Equal(t, 2, be.cntExec)
	require.Equal(t, 1, be.cntRepeat)
	retry, ok := queue.Next(t.Context())
	require.True(t, ok)
	require.Equal(t, 1, retry.Version().Incarnation)

	pe.recordBlockExecMetrics(be)
	execCount, repeatCount := pe.dispatchCounts.load()
	require.Equal(t, uint64(2), execCount)
	require.Equal(t, uint64(1), repeatCount)
}
