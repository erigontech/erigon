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
