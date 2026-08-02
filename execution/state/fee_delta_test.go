package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

// The fee-delta vector is a raw, coinbase-specific additive store indexed by
// TxIndex — a dense slice, fully populated by end of block (every tx credits the
// coinbase, zero tip included). It carries no versioning/OCC: summing it never
// records a dependency, so the universal coinbase credit never serializes. A
// case-(c) reader only ever sums a range the frontier has already populated;
// these tests pin the fold contract and that invariant.
func TestFeeDeltaVec_SumIsOrderIndependent(t *testing.T) {
	t.Parallel()
	f := NewFeeDeltaVec(5)
	// Write out of order — additivity must not depend on write order.
	f.Set(3, uint256.NewInt(30))
	f.Set(0, uint256.NewInt(10))
	f.Set(4, uint256.NewInt(40))
	f.Set(1, uint256.NewInt(20))
	f.Set(2, uint256.NewInt(25))

	// after=-1 → include tx 0: 10+20+25+30+40.
	total := f.Sum(-1, 4)
	assert.Equal(t, uint256.NewInt(125), &total)
}

func TestFeeDeltaVec_SumRangeAfterCheckpoint(t *testing.T) {
	t.Parallel()
	f := NewFeeDeltaVec(5)
	for i, v := range []uint64{10, 20, 25, 30, 40} {
		f.Set(i, uint256.NewInt(v))
	}
	// Checkpoint at tx 1 (its absolute already folds deltas 0..1): only (1,4] = 2..4.
	total := f.Sum(1, 4)
	assert.Equal(t, uint256.NewInt(25+30+40), &total, "sum is (checkpoint, floor]")
}

func TestFeeDeltaVec_SetLastWriteWins(t *testing.T) {
	t.Parallel()
	f := NewFeeDeltaVec(3)
	f.Set(0, uint256.NewInt(10))
	f.Set(1, uint256.NewInt(20))
	// tx 1 re-executes to a different tip — the raw cell is overwritten, no
	// incarnation tracking, no double-count.
	f.Set(1, uint256.NewInt(21))
	f.Set(2, uint256.NewInt(0))

	total := f.Sum(-1, 2)
	assert.Equal(t, uint256.NewInt(31), &total)
}

// Summing a range the frontier has not populated is a scheduling bug (a reader
// ran ahead of the coinbase frontier); the vector asserts rather than silently
// folding a short set — the failure mode that produced the residual.
func TestFeeDeltaVec_SumUnpopulatedRangePanics(t *testing.T) {
	t.Parallel()
	f := NewFeeDeltaVec(5)
	f.Set(0, uint256.NewInt(10))
	f.Set(1, uint256.NewInt(20))
	// tx 2 unwritten.
	assert.Panics(t, func() { f.Sum(-1, 3) }, "must assert on an unpopulated slot in range")
}
