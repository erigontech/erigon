package kvmetrics

import (
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// TestCollectorConcurrentSendAndDrain exercises the channel collector under many
// concurrent producers plus concurrent Snapshot, then Stop. Run with -race: it
// is the guard for the single-owner (no-lock) grouped map and the Close-drain.
func TestCollectorConcurrentSendAndDrain(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	var wg sync.WaitGroup
	c.Start(&wg)

	const producers = 8
	const perProducer = 500
	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			src := Source(p % int(sourceCount))
			for i := 0; i < perProducer; i++ {
				m := NewDomainMetrics()
				m.UpdateCacheReads(kv.AccountsDomain, time.Now())
				m.UpdateDbReads(kv.StorageDomain, time.Now())
				c.Send(src, m)
			}
		}(p)
	}
	// Concurrent snapshots while producers run.
	var swg sync.WaitGroup
	swg.Add(1)
	go func() {
		defer swg.Done()
		for i := 0; i < 50; i++ {
			_ = c.Snapshot()
		}
	}()

	pwg.Wait()
	swg.Wait()
	c.Stop() // drains the buffer
	wg.Wait()

	snap := c.Snapshot() // after Stop, returns nil
	if snap != nil {
		t.Fatalf("expected nil snapshot after Stop, got %v", snap)
	}
}

// TestCollectorFoldsBySource verifies the grouped totals are correct and that a
// drained Stop does not lose buffered samples.
func TestCollectorFoldsBySource(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	var wg sync.WaitGroup
	c.Start(&wg)

	const n = 1000
	want := int64(n)
	for i := 0; i < n; i++ {
		m := NewDomainMetrics()
		m.UpdateCacheReads(kv.AccountsDomain, time.Now())
		c.Send(SourceExec, m)
	}
	// Snapshot via the collector goroutine forces all prior sends to be folded
	// (channel-ordered), so the count is exact.
	snap := c.Snapshot()
	c.Stop()
	wg.Wait()

	g := snap[SourceExec]
	if g == nil {
		t.Fatal("no exec group")
	}
	if g.CacheReadCount != want {
		t.Fatalf("exec CacheReadCount = %d, want %d", g.CacheReadCount, want)
	}
	if d := g.Domains[kv.AccountsDomain]; d == nil || d.CacheReadCount != want {
		t.Fatalf("account-domain CacheReadCount mismatch: %+v", d)
	}
}
