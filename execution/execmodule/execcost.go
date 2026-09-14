package execmodule

import (
	"cmp"
	"slices"
	"sync"
	"time"
)

// execCostWindow is a sliding window of per-tx execution cost, read by the driver's batch sizer to decide how
// many txs to feed into the next pre-exec round: time bound ≈ budget / upperQuartileTimePerTx, gas bound ≈
// gasLimit / upperQuartileGasPerTx. Upper quartile (not average) so the bound stays conservative when a batch
// runs expensive. Nothing is gathered or persisted — the quartile is recomputed from the live window each time.
//
// It holds TWO INDEPENDENT windows, because only one place knows each quantity honestly:
//
//   - per-tx TIME comes from the PRE-EXEC round, and EVERY round is recorded. That is the work being sized.
//     Sampling the seal instead measured the close (321µs/tx) rather than the execution (1.09s/tx), and
//     sampling only blocks that SEALED meant a round which overran its budget contributed no sample at all —
//     the window could only ever learn from the fast rounds, and was structurally blind to the tail that
//     stalls block production.
//   - per-tx GAS comes from the SEAL, which is where the block's authoritative GasUsed is known.
//
// The two were never a pair in any case: upperQuartile has always sorted them independently.
type execCostWindow struct {
	mu    sync.Mutex
	times costRing[time.Duration]
	gases costRing[uint64]
}

func newExecCostWindow(capacity int) *execCostWindow {
	if capacity <= 0 {
		capacity = 64
	}
	return &execCostWindow{
		times: newCostRing[time.Duration](capacity),
		gases: newCostRing[uint64](capacity),
	}
}

// recordRoundTime adds one PRE-EXEC round's per-tx time. No-op for txs<=0 (an open/marker round executes no
// transactions, so it says nothing about per-tx cost).
func (w *execCostWindow) recordRoundTime(total time.Duration, txs int) {
	if w == nil || txs <= 0 || total <= 0 {
		return
	}
	w.mu.Lock()
	w.times.push(total / time.Duration(txs))
	w.mu.Unlock()
}

// recordSealGas adds one sealed block's per-tx gas.
func (w *execCostWindow) recordSealGas(gasUsed uint64, txs int) {
	if w == nil || txs <= 0 {
		return
	}
	w.mu.Lock()
	w.gases.push(gasUsed / uint64(txs))
	w.mu.Unlock()
}

// upperQuartile computes (dynamically, from the current windows) the 75th-percentile per-tx TIME and per-tx
// GAS. Either is 0 while its own window is empty.
func (w *execCostWindow) upperQuartile() (perTxTime time.Duration, perTxGas uint64) {
	if w == nil {
		return 0, 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.times.upperQuartile(), w.gases.upperQuartile()
}

// costRing is a fixed-capacity ring of samples that reports its own upper quartile.
type costRing[T cmp.Ordered] struct {
	vals   []T
	capn   int
	next   int
	filled bool
}

func newCostRing[T cmp.Ordered](capacity int) costRing[T] {
	return costRing[T]{vals: make([]T, capacity), capn: capacity}
}

func (r *costRing[T]) push(v T) {
	r.vals[r.next] = v
	r.next++
	if r.next >= r.capn {
		r.next = 0
		r.filled = true
	}
}

func (r *costRing[T]) upperQuartile() T {
	var zero T
	n := r.capn
	if !r.filled {
		n = r.next
	}
	if n == 0 {
		return zero
	}
	vals := make([]T, n)
	copy(vals, r.vals[:n])
	slices.Sort(vals)
	idx := (n * 3) / 4
	if idx >= n {
		idx = n - 1
	}
	return vals[idx]
}
