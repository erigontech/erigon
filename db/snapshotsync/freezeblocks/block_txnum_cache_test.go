package freezeblocks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapshotsync"
)

func TestBlockTxNumLookupCacheFind(t *testing.T) {
	t.Parallel()
	// Block b owns txNums [2b, 2b+1]. 1024 is an epoch segment size, which the cadence does not divide.
	maxTxNum := func(b uint64) (uint64, error) { return 2*b + 1, nil }
	for _, r := range []snapshotsync.Range{snapshotsync.NewRange(0, 1000), snapshotsync.NewRange(0, 1024), snapshotsync.NewRange(1024, 2048)} {
		cache := NewBlockTxNumLookupCache(20)
		for b := r.From(); b < r.To(); b++ {
			for _, txNum := range []uint64{2 * b, 2*b + 1} {
				got, err := cache.Find(r, txNum, maxTxNum)
				require.NoError(t, err)
				require.Equal(t, b, got, "range %d-%d, txNum %d", r.From(), r.To(), txNum)
			}
		}
	}
}
