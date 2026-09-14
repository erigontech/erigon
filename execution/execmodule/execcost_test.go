package execmodule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A round that overruns must be SAMPLED, not discarded — the whole point of the window is to predict it.
func TestExecCostWindow_SlowRoundsAreSampled(t *testing.T) {
	w := newExecCostWindow(8)
	for i := 0; i < 6; i++ {
		w.recordRoundTime(2*time.Millisecond, 1)
	}
	w.recordRoundTime(6*time.Second, 6) // one slow round: 1s/tx
	w.recordRoundTime(1100*time.Millisecond, 1)

	uqTime, _ := w.upperQuartile()
	require.GreaterOrEqual(t, uqTime, time.Second,
		"the upper quartile must reflect the slow rounds, not just the fast ones")
}

// Time comes from pre-exec rounds and gas from seals; each window fills on its own.
func TestExecCostWindow_TimeAndGasAreIndependent(t *testing.T) {
	w := newExecCostWindow(4)

	uqTime, uqGas := w.upperQuartile()
	require.Zero(t, uqTime)
	require.Zero(t, uqGas)

	w.recordRoundTime(400*time.Millisecond, 2) // 200ms/tx
	uqTime, uqGas = w.upperQuartile()
	require.Equal(t, 200*time.Millisecond, uqTime)
	require.Zero(t, uqGas, "a pre-exec round says nothing about the block's authoritative gas")

	w.recordSealGas(3_000_000, 2) // 1.5M/tx
	uqTime, uqGas = w.upperQuartile()
	require.Equal(t, 200*time.Millisecond, uqTime, "a seal must not move the time estimate")
	require.Equal(t, uint64(1_500_000), uqGas)
}

// Rounds that execute no transactions (open, marker) say nothing about per-tx cost.
func TestExecCostWindow_EmptyRoundsIgnored(t *testing.T) {
	w := newExecCostWindow(4)
	w.recordRoundTime(500*time.Millisecond, 0)
	w.recordSealGas(21_000, 0)
	uqTime, uqGas := w.upperQuartile()
	require.Zero(t, uqTime)
	require.Zero(t, uqGas)
}

func TestExecCostWindow_WrapsAndForgets(t *testing.T) {
	w := newExecCostWindow(4)
	w.recordRoundTime(9*time.Second, 1)
	for i := 0; i < 4; i++ {
		w.recordRoundTime(time.Millisecond, 1)
	}
	uqTime, _ := w.upperQuartile()
	require.Equal(t, time.Millisecond, uqTime, "the 9s sample must have been evicted")
}

func TestExecCostWindow_NilIsSafe(t *testing.T) {
	var w *execCostWindow
	w.recordRoundTime(time.Second, 1)
	w.recordSealGas(21_000, 1)
	uqTime, uqGas := w.upperQuartile()
	require.Zero(t, uqTime)
	require.Zero(t, uqGas)
}
