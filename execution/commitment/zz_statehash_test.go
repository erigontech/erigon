package commitment

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

func TestStateHashMemoHitRate(t *testing.T) {
	pk, upds := buildWhaleCorpus(whale1M())
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
	sl, hl, hr := skippedLoad.Load(), hadToLoad.Load(), hadToReset.Load()
	u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, dk, du)
	_, err = hph.Process(ctx, u2, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2.Close()

	dsl := skippedLoad.Load() - sl
	dhl := hadToLoad.Load() - hl
	dhr := hadToReset.Load() - hr
	v := hph.Metrics().AsValues()
	t.Logf("delta round: skippedLoad=%d hadToLoad=%d hadToReset=%d", dsl, dhl, dhr)
	t.Logf("  stateHash memo hit rate = %.1f%%  (skipped / (skipped+load))",
		100*float64(dsl)/float64(dsl+dhl))
	t.Logf("  loadAccount=%d loadStorage=%d for %d delta keys", v.LoadAccount, v.LoadStorage, len(dk))
}
