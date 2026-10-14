package execmodule

import (
	"cmp"
	"math/bits"
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
	sizes costRing[int] // the round size each times sample came from, pushed in step
	gases costRing[uint64]
}

// roundCost fits elapsed = flat + perTx*n over the window, by least squares. Measured per round on
// live309, one drain's eighteen rounds:
//
//	22:93 36:105 30:68 26:62 23:60 20:55 17:51 14:44 12:33 10:29 8:32 6:25 5:18 4:15 3:16 2:15 1:15 1:14
//
// which fits flat=9.2ms, perTx=2.43ms — and a 1-transaction round does cost 14-15ms while a 30-transaction
// round costs 2.3ms each. A round therefore has a cost that does not scale with its size, and charging
// that flat part to every transaction is what collapses the end of a drain: the caller's clamp is
// left/perTx with perTx a p75 of total/n, which the small rounds drag to 18ms, so 100ms of budget is
// said to fit 5 transactions where it fits about 37. The back half of every block was then spent paying
// the flat cost again and again for one or two transactions at a time.
//
// Returns (0,0) when there is not enough to fit, or when the fit comes out degenerate — the caller keeps
// its own estimate then rather than acting on a slope invented from four points.
func (w *execCostWindow) roundCost() (flat, perTx time.Duration) {
	if w == nil {
		return 0, 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	n := w.times.capn
	if !w.times.filled {
		n = w.times.next
	}
	if n < 8 {
		return 0, 0
	}
	var sx, sy, sxx, sxy float64
	cnt := 0
	for i := range n {
		sz := w.sizes.vals[i]
		if sz <= 0 {
			continue
		}
		x := float64(sz)
		// times holds total/n, so the round's own elapsed is that back times its size. Fitting the
		// per-tx figure directly would fit a hyperbola with a straight line and report nonsense.
		y := float64(w.times.vals[i]) * x
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
		cnt++
	}
	if cnt < 8 {
		return 0, 0
	}
	n = cnt
	den := float64(n)*sxx - sx*sx
	if den == 0 {
		return 0, 0
	}
	p := (float64(n)*sxy - sx*sy) / den
	f := (sy - p*sx) / float64(n)
	if p <= 0 {
		return 0, 0
	}
	if f < 0 {
		f = 0
	}
	return time.Duration(f), time.Duration(p)
}

func newExecCostWindow(capacity int) *execCostWindow {
	if capacity <= 0 {
		capacity = 64
	}
	return &execCostWindow{
		times: newCostRing[time.Duration](capacity),
		sizes: newCostRing[int](capacity),
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
	// The ROUND's elapsed and its size, in step, so roundCost can separate the flat part from the
	// marginal one. times keeps total/n because upperQuartile still reports that.
	w.times.push(total / time.Duration(txs))
	w.sizes.push(txs)
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

// gasOutturn maps a transaction's ASKED gas — its declared limit, bucketed — to what transactions of that
// size actually USED. The declared limit is a biased estimator and biased one way only: a caller that sets a
// fixed generous maximum asks for the same number every time, whatever the work turns out to be, and the
// sponsored order path does exactly that. Sizing a block off the asked number then leaves it part-empty by
// the whole margin.
//
// IT IS A SOFT DISTINGUISHER AND DOES NOT NEED TO BE ACCURATE. The bucket is the asked value's magnitude —
// its bit length — which separates classes of transaction without decoding calldata or knowing anything
// about the contract: a run of fixed-max transactions all lands in one bucket and the samples there describe
// it. A bucket with no samples answers with the asked value, which is the conservative way to be wrong.
type gasOutturn struct {
	mu      sync.Mutex
	capn    int
	buckets map[int]*costRing[uint64]
}

func newGasOutturn(capacity int) *gasOutturn {
	if capacity <= 0 {
		capacity = 64
	}
	return &gasOutturn{capn: capacity, buckets: make(map[int]*costRing[uint64])}
}

// gasBucket keys by magnitude, so neighbouring limits share a bucket and a fixed limit gets one of its own.
func gasBucket(asked uint64) int { return bits.Len64(asked) }

func (g *gasOutturn) record(asked, used uint64) {
	if g == nil || asked == 0 {
		return
	}
	k := gasBucket(asked)
	g.mu.Lock()
	r, ok := g.buckets[k]
	if !ok {
		nr := newCostRing[uint64](g.capn)
		r = &nr
		g.buckets[k] = r
	}
	r.push(used)
	g.mu.Unlock()
}

// effective is what a transaction asking for this much should be SIZED at, given how much gas the block has
// LEFT. It slides between the two honest answers rather than picking one:
//
//   - With the block nearly empty it uses the OUTTURN — what transactions of this size actually spend. Being
//     wrong here is cheap: the block has room to absorb it, and the alternative is leaving most of the block
//     empty because every caller declared a generous maximum.
//   - With the block nearly full it uses the ASKED value. Being wrong here is not cheap — a transaction that
//     spends its whole declared limit would take the block past its gas limit — so the last transactions in
//     are sized at what they could cost, not at what they probably will.
//
// The blend is linear in the room remaining, so there is no threshold to tune and no step for a block to sit
// astride. Never above asked: the chain enforces that ceiling anyway.
func (g *gasOutturn) effective(asked, gasRoom, gasLimit uint64) uint64 {
	if g == nil || asked == 0 {
		return asked
	}
	g.mu.Lock()
	r, ok := g.buckets[gasBucket(asked)]
	g.mu.Unlock()
	if !ok {
		return asked
	}
	uq := r.upperQuartile()
	if uq == 0 || uq >= asked {
		return asked
	}
	if gasLimit == 0 || gasRoom >= gasLimit {
		return uq
	}
	// spent/limit of the way through the block, spent/limit of the way from the outturn to the asked value.
	spent := gasLimit - gasRoom
	return uq + (asked-uq)*spent/gasLimit
}

// perTxUpperQuartile is the 75th-percentile USED gas across every bucket — the per-transaction gas the batch
// sizer wants. It is sampled once per TRANSACTION, which is what makes it able to track a burst: the seal-time
// window it replaces took one sample per sealed BLOCK over 64 blocks, so a burst of ten or twenty blocks never
// displaced the idle blocks already in it. Measured on live298 and live299, that window reported the same
// value to the last digit for an entire run — 1,001,635 and 1,512,066 — while the loaded blocks it was
// supposedly describing ran at 451,870 gas per transaction.
func (g *gasOutturn) perTxUpperQuartile() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	var all []uint64
	for _, r := range g.buckets {
		n := r.capn
		if !r.filled {
			n = r.next
		}
		all = append(all, r.vals[:n]...)
	}
	g.mu.Unlock()
	if len(all) == 0 {
		return 0
	}
	slices.Sort(all)
	idx := (len(all) * 3) / 4
	if idx >= len(all) {
		idx = len(all) - 1
	}
	return all[idx]
}
