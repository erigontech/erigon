package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/blockreplay"
)

func TestValidationSkipFiresOnReplayedBlock(t *testing.T) {
	fx, err := blockreplay.Load(fixturePath(t))
	require.NoError(t, err)
	require.NotNil(t, fx.Outputs)

	r, closeFn := setupEphemeralReplay(t, fx)
	defer closeFn()
	tx, doms := r.newDomains(t, fx)
	defer tx.Rollback()
	defer doms.Close()

	before := valSkipCount.Load()
	require.NoError(t, r.exec(tx, doms))
	require.NoError(t, r.verify(tx, doms, fx.Outputs))
	skipped := valSkipCount.Load() - before
	require.Positive(t, skipped, "no arrival validation was skipped on a normal block")
	t.Logf("skipped %d of %d txs", skipped, len(r.block.Transactions()))
}
