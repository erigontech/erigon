package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The case this exists for: a caller that declares a fixed generous maximum for every transaction. Sizing a
// block off the declared number leaves it empty by the whole margin, and the margin here is 6.6x.
func TestGasOutturn_FixedMaxIsCorrectedToWhatItSpends(t *testing.T) {
	const (
		asked    = uint64(3_000_000)
		spent    = uint64(451_870)
		gasLimit = uint64(300_000_000)
	)
	g := newGasOutturn(64)

	// No samples yet: answer with what it asked for. Being conservative is the right way to be wrong.
	require.Equal(t, asked, g.effective(asked, gasLimit, gasLimit))

	for range 40 {
		g.record(asked, spent)
	}
	require.Equal(t, spent, g.effective(asked, gasLimit, gasLimit), "an empty block sizes by the outturn")
}

// Optimistic at the start of the block, conservative at the end: an over-run with the block nearly empty is
// absorbed, an over-run at the limit would push the block past it.
func TestGasOutturn_SlidesFromOutturnToAskedAsTheBlockFills(t *testing.T) {
	const (
		asked    = uint64(3_000_000)
		spent    = uint64(450_000)
		gasLimit = uint64(300_000_000)
	)
	g := newGasOutturn(64)
	for range 40 {
		g.record(asked, spent)
	}

	require.Equal(t, spent, g.effective(asked, gasLimit, gasLimit), "empty block: the outturn")
	require.Equal(t, asked, g.effective(asked, 0, gasLimit), "full block: what it could cost")

	half := g.effective(asked, gasLimit/2, gasLimit)
	require.Greater(t, half, spent)
	require.Less(t, half, asked)

	prev := spent
	for room := gasLimit; ; room -= gasLimit / 10 {
		cur := g.effective(asked, room, gasLimit)
		require.GreaterOrEqual(t, cur, prev, "it only ever becomes more conservative (room=%d)", room)
		require.LessOrEqual(t, cur, asked, "never above the declared limit")
		prev = cur
		if room < gasLimit/10 {
			break
		}
	}
}

// Buckets separate classes without knowing anything about the contract: a cheap transfer and an expensive
// order declare different magnitudes, so one cannot be corrected by the other's samples.
func TestGasOutturn_BucketsSeparateClasses(t *testing.T) {
	const gasLimit = uint64(300_000_000)
	g := newGasOutturn(64)
	for range 40 {
		g.record(3_000_000, 450_000)
	}
	require.Equal(t, uint64(21_000), g.effective(21_000, gasLimit, gasLimit),
		"a transfer is not described by the order bucket's samples")
}

// The per-transaction quartile is what replaces the per-sealed-block window. One sample per block could not
// move inside a burst; this moves with every transaction.
func TestGasOutturn_PerTxQuartileTracksTheLoad(t *testing.T) {
	g := newGasOutturn(256)
	require.Equal(t, uint64(0), g.perTxUpperQuartile(), "no samples, no answer")
	for range 200 {
		g.record(3_000_000, 450_000)
	}
	require.Equal(t, uint64(450_000), g.perTxUpperQuartile())
}
