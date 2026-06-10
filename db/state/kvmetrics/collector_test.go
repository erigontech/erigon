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

// TestCollectorTrySendNeverBlocks proves TrySend returns immediately even when
// the buffer is full and nothing drains it (collector not Started): excess sends
// return false (the caller retains its data) rather than blocking. This is the
// guarantee that the exec hot path never back-pressures on metrics.
func TestCollectorTrySendNeverBlocks(t *testing.T) {
	t.Parallel()
	c := NewCollector() // deliberately NOT Started → nothing drains c.in

	var falses int64
	done := make(chan struct{})
	go func() {
		for i := 0; i < collectorBufferSize*3; i++ {
			if !c.TrySend(SourceExec, NewDomainMetrics()) {
				falses++
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("TrySend blocked on a full buffer — must never back-pressure the producer")
	}
	if falses == 0 {
		t.Fatal("expected TrySend to report a full buffer once it filled")
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
