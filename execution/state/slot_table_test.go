package state

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSharedSlotTableConcurrentInternGivesOneIDPerKey(t *testing.T) {
	const workers, keys = 16, 512
	table := newSharedSlotTable(0)
	got := make([][]slotID, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			ids := make([]slotID, keys)
			for i := range keys {
				ids[(i+w*31)%keys] = table.intern(accounts.StorageKey{byte(((i + w*31) % keys) >> 8), byte((i + w*31) % keys)})
			}
			got[w] = ids
		})
	}
	wg.Wait()
	seen := make(map[slotID]struct{}, keys)
	for i := range keys {
		id := got[0][i]
		require.Equal(t, accounts.StorageKey{byte(i >> 8), byte(i)}, *id)
		for w := 1; w < workers; w++ {
			require.Same(t, id, got[w][i], "key %d", i)
		}
		seen[id] = struct{}{}
	}
	require.Len(t, seen, keys)
}

func TestSlotTableAdoptTranslatesAcrossTables(t *testing.T) {
	shared, private := newSharedSlotTable(0), &slotTable{}
	k := accounts.StorageKey{0x01}
	sid := shared.intern(k)
	pid := private.intern(k)
	require.NotSame(t, sid, pid)
	require.Same(t, sid, shared.adopt(shared, sid))
	require.Same(t, pid, private.adopt(shared, sid))
	require.Same(t, sid, shared.adopt(private, pid))

	_, ok := private.findFrom(shared, shared.intern(accounts.StorageKey{0x02}))
	require.False(t, ok)
	id, ok := shared.find(k)
	require.True(t, ok)
	require.Same(t, sid, id)
}
