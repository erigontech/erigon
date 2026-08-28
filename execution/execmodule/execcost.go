package execmodule

import (
	"sort"
	"sync"
	"time"
)

// execCostWindow is a sliding window of per-tx execution cost — per-tx seal time and per-tx gas — over the most
// recent sealed (non-empty) blocks. Its UPPER QUARTILE is computed dynamically (on demand, from the current
// window) and read by the driver to size the NEXT batch of txs it feeds into exec, so a block always fits the two
// constraints on where to cut the block off:
//   - RELATIVE (time): maxTxs ≈ timeout / upperQuartileTimePerTx
//   - ABSOLUTE (gas):  bound by each tx's gas estimate against the gas limit (upperQuartileGasPerTx is the
//     execution-measured cross-check).
//
// Upper quartile (not average) so the bound stays conservative when a batch runs expensive. Nothing is gathered
// or persisted — the quartile is just recomputed from the live window each time it is asked for.
type execCostSample struct {
	perTxTime time.Duration
	perTxGas  uint64
}

type execCostWindow struct {
	mu      sync.Mutex
	samples []execCostSample
	capn    int
	next    int
	filled  bool
}

func newExecCostWindow(capacity int) *execCostWindow {
	if capacity <= 0 {
		capacity = 64
	}
	return &execCostWindow{samples: make([]execCostSample, capacity), capn: capacity}
}

// record adds one sealed block's per-tx cost (total seal time and total gas over its tx count). No-op for txs<=0.
func (w *execCostWindow) record(total time.Duration, gasUsed uint64, txs int) {
	if txs <= 0 || total <= 0 {
		return
	}
	s := execCostSample{perTxTime: total / time.Duration(txs), perTxGas: gasUsed / uint64(txs)}
	w.mu.Lock()
	w.samples[w.next] = s
	w.next++
	if w.next >= w.capn {
		w.next = 0
		w.filled = true
	}
	w.mu.Unlock()
}

// upperQuartile computes (dynamically, from the current window) the 75th-percentile per-tx TIME and per-tx GAS
// (0,0 when empty).
func (w *execCostWindow) upperQuartile() (perTxTime time.Duration, perTxGas uint64) {
	w.mu.Lock()
	n := w.capn
	if !w.filled {
		n = w.next
	}
	if n == 0 {
		w.mu.Unlock()
		return 0, 0
	}
	times := make([]time.Duration, n)
	gases := make([]uint64, n)
	for i := 0; i < n; i++ {
		times[i] = w.samples[i].perTxTime
		gases[i] = w.samples[i].perTxGas
	}
	w.mu.Unlock()
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	sort.Slice(gases, func(i, j int) bool { return gases[i] < gases[j] })
	idx := (n * 3) / 4
	if idx >= n {
		idx = n - 1
	}
	return times[idx], gases[idx]
}
