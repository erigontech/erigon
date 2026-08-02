package state

import (
	"fmt"
	"sync/atomic"

	"github.com/holiman/uint256"
)

// FeeDeltaVec is a raw, coinbase-specific additive store indexed by TxIndex: a
// dense vector, fully populated by end of block (every tx credits the coinbase,
// zero tip included). It carries no versioning/OCC — summing it records no
// dependency, so the universal coinbase credit never serializes.
//
// Concurrency: each TxIndex is written by exactly one worker (its own tx). The
// atomic written marker publishes the value write to a later summing reader
// (release/acquire), and a reader only ever sums a range the coinbase frontier
// has already populated — Sum asserts that rather than folding a short set.
type FeeDeltaVec struct {
	delta   []uint256.Int
	written []atomic.Bool
}

func NewFeeDeltaVec(nTx int) *FeeDeltaVec {
	return &FeeDeltaVec{
		delta:   make([]uint256.Int, nTx),
		written: make([]atomic.Bool, nTx),
	}
}

// Set records tx txIdx's raw fee delta. Last-write-wins: a re-execution
// overwrites the prior incarnation's tip, so no incarnation tracking is needed.
func (f *FeeDeltaVec) Set(txIdx int, delta *uint256.Int) {
	f.delta[txIdx].Set(delta)
	f.written[txIdx].Store(true)
}

// Sum totals the deltas over (afterTxIdx, floorTxIdx]. afterTxIdx is the last
// sum point — a BalancePath checkpoint whose absolute already folds earlier
// deltas — so pass -1 to include tx 0. Panics if any slot in range is
// unpopulated: that means a reader ran ahead of the coinbase frontier, the
// scheduling bug that produced the residual.
func (f *FeeDeltaVec) Sum(afterTxIdx, floorTxIdx int) uint256.Int {
	var total uint256.Int
	for i := afterTxIdx + 1; i <= floorTxIdx; i++ {
		if !f.written[i].Load() {
			panic(fmt.Sprintf("FeeDeltaVec.Sum: tx %d unpopulated in range (%d,%d]", i, afterTxIdx, floorTxIdx))
		}
		total.Add(&total, &f.delta[i])
	}
	return total
}
