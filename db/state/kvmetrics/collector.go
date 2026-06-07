package kvmetrics

import (
	"sync"
	"time"
)

// Source identifies which read path produced a batch of metrics, so the
// Collector can group (and Prometheus can label) IO by subsystem. The previous
// design only ever metered exec + commitment-during-exec; with the channel
// collector every read path contributes, tagged by Source.
type Source uint8

const (
	SourceExec Source = iota
	SourceCommitment
	SourceWarmup
	SourceRPC
	SourceEngine
	sourceCount
)

// String is the Prometheus label value for the source.
func (s Source) String() string {
	switch s {
	case SourceExec:
		return "exec"
	case SourceCommitment:
		return "commitment"
	case SourceWarmup:
		return "warmup"
	case SourceRPC:
		return "rpc"
	case SourceEngine:
		return "engine"
	default:
		return "unknown"
	}
}

// MetricsCollectorProvider is implemented by *state.AggregatorRoTx. SharedDomains
// fetches the process-level collector through tx.AggTx().(MetricsCollectorProvider)
// — the same duck-type pattern as commitment.BranchCacheProvider — so the leaf
// kvmetrics package need not be imported into a cycle with db/state.
type MetricsCollectorProvider interface {
	MetricsCollector() *Collector
}

// sample carries a finished per-worker instance to the collector. Ownership of m
// transfers on send: the producer must not touch m afterwards.
type sample struct {
	source Source
	m      *DomainMetrics
}

// collectorBufferSize bounds the channel. Sends are coarse — one per exec task,
// one per ComputeCommitment, one per warmup/mount teardown, one per RPC/engine
// request — so peak in-flight is worker-count × in-flight-tasks (tens to low
// hundreds). 4096 gives wide headroom so a producer never blocks on the hot
// path; if it ever fills, the send blocks (correct back-pressure) which is rare.
const collectorBufferSize = 4096

// Collector is the process-level, single-goroutine aggregator. Producers fill
// their own *DomainMetrics lock-free and Send() it (ownership transfer); the
// collector goroutine folds each into grouped[source]. grouped is touched only
// by that goroutine, so the aggregate needs no mutex/atomics — this is what
// makes the whole metrics path -race clean without a per-read lock.
//
// Owned by the Aggregator: Start() at open, Stop() at close (drains the buffer
// so no buffered sample is lost). See db/state/aggregator.go.
type Collector struct {
	in       chan sample
	snapReq  chan chan map[Source]*DomainMetrics
	quit     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	grouped  map[Source]*DomainMetrics // single-owner: only run() touches it
}

func NewCollector() *Collector {
	return &Collector{
		in:      make(chan sample, collectorBufferSize),
		snapReq: make(chan chan map[Source]*DomainMetrics),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		grouped: make(map[Source]*DomainMetrics),
	}
}

// Start launches the collector goroutine, registered on the Aggregator's
// WaitGroup so Close joins it.
func (c *Collector) Start(wg *sync.WaitGroup) {
	if c == nil {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.run()
	}()
}

func (c *Collector) run() {
	ticker := time.NewTicker(publishInterval)
	defer ticker.Stop()
	for {
		select {
		case s := <-c.in:
			c.fold(s)
		case rc := <-c.snapReq:
			c.drain() // fold everything already buffered so the snapshot is current
			rc <- c.snapshot()
		case <-ticker.C:
			c.publish()
		case <-c.quit:
			c.drain()
			c.publish() // final push so the last interval's reads aren't lost
			close(c.done)
			return
		}
	}
}

func (c *Collector) fold(s sample) {
	if s.m == nil {
		return
	}
	g := c.grouped[s.source]
	if g == nil {
		g = NewDomainMetrics()
		c.grouped[s.source] = g
	}
	g.mergeLocked(s.m) // single-owner: no lock needed
}

// drain folds everything still buffered. Runs on the collector goroutine when
// quit fires. Correctness rests on the contract that no producer sends after
// Stop() (exec workers are joined before Aggregator.Close), so the buffer is a
// finite set of already-queued samples.
func (c *Collector) drain() {
	for {
		select {
		case s := <-c.in:
			c.fold(s)
		default:
			return
		}
	}
}

func (c *Collector) snapshot() map[Source]*DomainMetrics {
	out := make(map[Source]*DomainMetrics, len(c.grouped))
	for src, g := range c.grouped {
		cp := NewDomainMetrics()
		cp.DomainIOMetrics = g.DomainIOMetrics
		for d, dm := range g.Domains {
			e := *dm
			cp.Domains[d] = &e
		}
		out[src] = cp
	}
	return out
}

// TrySend transfers ownership of m to the collector tagged with source, WITHOUT
// blocking. Returns true if it was queued (the caller must then allocate a fresh
// instance and not touch m again), false if the buffer was momentarily full (the
// caller retains ownership of m and should keep accumulating into it, retrying
// later — nothing is dropped). This is the hot-path send (exec workers): metrics
// never block execution and never lose counts. nil collector/m → false (the
// caller keeps its data).
func (c *Collector) TrySend(source Source, m *DomainMetrics) bool {
	if c == nil || m == nil {
		return false
	}
	select {
	case c.in <- sample{source: source, m: m}:
		return true
	default:
		return false
	}
}

// Send transfers ownership of m to the collector, blocking until it is queued.
// For low-frequency boundary producers (commitment fold, warmup teardown, an RPC
// request closing) that are off the per-tx hot path, where a brief, rare wait is
// acceptable and losing the sample is not. nil collector/m is a no-op.
func (c *Collector) Send(source Source, m *DomainMetrics) {
	if c == nil || m == nil {
		return
	}
	select {
	case c.in <- sample{source: source, m: m}:
	case <-c.quit:
		// Collector is stopping (Aggregator.Close); no producer should still be
		// sending. Give up rather than block forever on a buffer no one drains.
	}
}

// Snapshot returns a deep copy of the grouped aggregate, produced by the
// collector goroutine itself (so grouped is never read concurrently — no lock).
// Returns nil once the collector has stopped.
func (c *Collector) Snapshot() map[Source]*DomainMetrics {
	if c == nil {
		return nil
	}
	rc := make(chan map[Source]*DomainMetrics, 1)
	select {
	case c.snapReq <- rc:
		return <-rc
	case <-c.quit:
		return nil
	}
}

// Stop signals the collector to drain and exit, then waits for it. Idempotent.
// Called by Aggregator.Close before WaitGroup.Wait().
func (c *Collector) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		close(c.quit)
		<-c.done
	})
}
