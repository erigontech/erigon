package state

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// A returned cache must survive GC cycles: the free-list must not be drained
// the way sync.Pool is, else warm caches are rebuilt at whole-process GC cadence.
func TestDomainGetFromFileCacheSurvivesGC(t *testing.T) {
	prevLimit, prevEnabled := domainGetFromFileCacheLimit, domainGetFromFileCacheEnabled
	domainGetFromFileCacheLimit, domainGetFromFileCacheEnabled = 128, true
	t.Cleanup(func() {
		domainGetFromFileCacheLimit, domainGetFromFileCacheEnabled = prevLimit, prevEnabled
	})

	v := newDomainVisible(kv.AccountsDomain, nil)

	c := v.newGetFromFileCache()
	require.NotNil(t, c)
	c.Add(1, domainGetFromFileCacheItem{lvl: 3, v: []byte{42}})
	v.returnGetFromFileCache(c)

	runtime.GC()
	runtime.GC()

	got := v.newGetFromFileCache()
	require.Same(t, c, got)
	item, ok := got.Get(1)
	require.True(t, ok)
	require.Equal(t, uint8(3), item.lvl)
	v.returnGetFromFileCache(got)
}
