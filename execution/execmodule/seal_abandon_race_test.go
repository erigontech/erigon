package execmodule_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

func TestAbandoningTheBuildWhileItIsBeingSealedLeavesTheSealWhole(t *testing.T) {
	h := newRoundHarness(t)
	parent, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)

	const txs = 32
	var nonces [2]uint64
	for i := 0; i < 40; i++ {
		params, in := h.attrs(parent)
		round := make([][]byte, 0, txs)
		for n := 0; n < txs; n++ {
			round = append(round, h.signed(n%2, nonces[n%2]+uint64(n/2), nil, 100_000, emitsALog))
		}
		_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, round)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)

		var wg sync.WaitGroup
		var sealed *types.BlockWithReceipts
		var sealErr, abandonErr error
		wg.Go(func() { sealed, sealErr = h.m.ExecModule.SealBlock(h.ctx, params, false) })
		delay := time.Duration(i%20) * 250 * time.Microsecond
		wg.Go(func() {
			time.Sleep(delay)
			abandonErr = h.m.ExecModule.AbandonFlashblock(h.ctx)
		})
		wg.Wait()
		require.NoError(t, sealErr, "iteration %d", i)
		require.NoError(t, abandonErr, "iteration %d", i)
		require.NotNil(t, sealed, "iteration %d", i)

		vr, err = validateChain(h.ctx, h.m.ExecModule, sealed.Block)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "iteration %d: %s", i, vr.ValidationError)
		ur, err := forkChoiceTo(h.ctx, h.m.ExecModule, sealed.Block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, "iteration %d", i)
		included := len(sealed.Block.Transactions())
		nonces[0] += uint64((included + 1) / 2)
		nonces[1] += uint64(included / 2)
		parent = sealed.Block.Header()
	}
}
