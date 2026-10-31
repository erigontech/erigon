package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/types"
)

// BenchmarkEphemeralBALReplay times parallel ExecV3 of a fixture block with the
// versionMap pre-seeded from the block's own BAL, the path a BAL block takes at the
// chain tip. The BAL is derived once, untimed; every timed run is checked against
// the fixture's canonical outputs and must reproduce the same BAL.
func BenchmarkEphemeralBALReplay(b *testing.B) {
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	r, closeFn := setupEphemeralReplay(b, fx)
	defer closeFn()

	r.cfg.experimentalBAL = true
	var captured types.BlockAccessList
	r.cfg.SetBALSink(func(_ uint64, bal types.BlockAccessList) { captured = bal })

	tx, doms := r.newDomains(b, fx)
	require.NoError(b, r.execSeeded(tx, doms, nil))
	require.NoError(b, r.verify(tx, doms, expected))
	doms.Close()
	tx.Rollback()
	require.NotNil(b, captured, "no BAL derived (experimentalBAL not honored?)")
	seed := captured
	want := seed.Hash()

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		captured = nil
		tx, doms := r.newDomains(b, fx)
		b.StartTimer()

		execErr := r.execSeeded(tx, doms, seed)

		b.StopTimer()
		require.NoError(b, execErr)
		require.NoError(b, r.verify(tx, doms, expected))
		require.NotNil(b, captured)
		require.Equal(b, want, captured.Hash(), "seeded run derived a different BAL")
		doms.Close()
		tx.Rollback()
	}
}
