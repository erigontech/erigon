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

// The eighteen rounds of one real drain on live309. A round costs a flat part plus a marginal part, and
// the flat part is what makes a 1-transaction round cost 14ms while a 30-transaction round costs 2.3ms
// each. Charging the flat part to every transaction — which left/perTx does — says 100ms of budget fits
// 5 transactions when it fits about 37, and that is what collapses the end of every drain.
func TestExecCostWindow_SeparatesFlatRoundCostFromMarginal(t *testing.T) {
	drain := []struct {
		n  int
		ms int
	}{
		{22, 93}, {36, 105}, {30, 68}, {26, 62}, {23, 60}, {20, 55}, {17, 51}, {14, 44}, {12, 33},
		{10, 29}, {8, 32}, {6, 25}, {5, 18}, {4, 15}, {3, 16}, {2, 15}, {1, 15}, {1, 14},
	}
	w := newExecCostWindow(64)
	for _, r := range drain {
		w.recordRoundTime(time.Duration(r.ms)*time.Millisecond, r.n)
	}

	flat, perTx := w.roundCost()
	require.InDelta(t, float64(9*time.Millisecond), float64(flat), float64(3*time.Millisecond),
		"the flat part is real: a 1-tx round costs 14-15ms")
	require.InDelta(t, float64(2400*time.Microsecond), float64(perTx), float64(600*time.Microsecond),
		"and the marginal part is what another transaction actually adds")

	// What the budget really holds, against what charging the flat cost to every transaction claims. The
	// gap here is modest because this window holds ONE clean drain; live it is far wider, because the 64
	// samples span many drains and their 1- and 2-transaction tails drag the p75 to 18.4ms — 5 transactions
	// in 100ms against the 37 that fit.
	left := 100 * time.Millisecond
	modelled := int((left - flat) / perTx)
	uq, _ := w.upperQuartile()
	naive := int(left / uq)
	require.Greater(t, modelled, naive, "charging the flat cost per transaction under-counts what fits")
}

// Too few rounds to fit, or a degenerate fit, must say so rather than hand back a slope invented from a
// handful of points — the caller keeps its own estimate then.
func TestExecCostWindow_RoundCostRefusesWhenItCannotFit(t *testing.T) {
	w := newExecCostWindow(64)
	for range 4 {
		w.recordRoundTime(20*time.Millisecond, 5)
	}
	flat, perTx := w.roundCost()
	require.Zero(t, flat)
	require.Zero(t, perTx)

	// Every round the same size: no spread to fit a slope through.
	w2 := newExecCostWindow(64)
	for range 20 {
		w2.recordRoundTime(50*time.Millisecond, 10)
	}
	f2, p2 := w2.roundCost()
	require.Zero(t, f2)
	require.Zero(t, p2)
}
