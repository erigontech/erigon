package commitment

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

func TestIncrementalRoundShape(t *testing.T) {
	for _, c := range whaleCases() {
		t.Run(c.name, func(t *testing.T) {
			pk, upds := buildWhaleCorpus(c.opts)
			dk, du := buildDelta(pk, upds, 500, 4242)
			ctx := context.Background()

			ms := NewMockState(t)
			require.NoError(t, ms.applyPlainUpdates(pk, upds))
			hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
			_, err := hph.Process(ctx, u1, "", nil, WarmupConfig{})
			require.NoError(t, err)
			u1.Close()

			require.NoError(t, ms.applyPlainUpdates(dk, du))
			u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, dk, du)
			_, err = hph.Process(ctx, u2, "", nil, WarmupConfig{})
			require.NoError(t, err)
			u2.Close()

			v := hph.Metrics().AsValues()
			t.Logf("baseKeys=%d deltaKeys=%d", len(pk), len(dk))
			t.Logf("  unfolds=%d folds=%d", v.Unfolds, v.Folds)
			t.Logf("  loadBranch=%d loadAccount=%d loadStorage=%d", v.LoadBranch, v.LoadAccount, v.LoadStorage)
			t.Logf("  updateBranch=%d", v.UpdateBranch)
			t.Logf("  branchReadBytes=%d branchWriteBytes=%d", v.BranchReadBytes, v.BranchWriteBytes)
			t.Logf("  perDeltaKey: unfolds=%.1f branchReads=%.1f readBytes=%.0f writeBytes=%.0f",
				float64(v.Unfolds)/float64(len(dk)),
				float64(v.LoadBranch)/float64(len(dk)),
				float64(v.BranchReadBytes)/float64(len(dk)),
				float64(v.BranchWriteBytes)/float64(len(dk)))
		})
	}
}
