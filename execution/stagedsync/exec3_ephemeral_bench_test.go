package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/blockreplay"
)

// BenchmarkEphemeralParallelReplay times ONLY the parallel ExecV3 call: setup,
// the per-run witness SharedDomains, and the post-state check are excluded via
// StopTimer/StartTimer. Each run is checked against the fixture's authoritative
// canonical outputs, so the measurement is of verified-correct execution.
func BenchmarkEphemeralParallelReplay(b *testing.B) {
	// Exec-only is set on cfg by setupEphemeralReplay; no env needed.
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	r, closeFn := setupEphemeralReplay(b, fx)
	defer closeFn()

	b.ResetTimer()
	for range b.N {
		func() {
			b.StopTimer()
			tx, doms, writeSet := r.newDomains(b, fx)
			// Deferred so a require failure (which Goexits) still closes the tx
			// before the test-DB cleanup, which otherwise blocks on the open tx.
			defer tx.Rollback()
			defer doms.Close()
			b.StartTimer()

			execErr := r.exec(tx, doms)

			b.StopTimer()
			require.NoError(b, execErr)
			require.NoError(b, r.verify(tx, doms, writeSet, expected))
		}()
	}
}
