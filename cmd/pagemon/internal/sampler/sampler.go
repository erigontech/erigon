package sampler

import (
	"sync"
	"time"

	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
)

// Snapshot is a single mincore reading taken at a point in time.
type Snapshot struct {
	At        time.Duration // elapsed since sampling started
	NewPages  int64         // pages newly loaded vs the baseline passed to New
	Residency []bool        // full bitmap, kept for temporal phase analysis
	FileSize  int64
	Sampled   bool // true if huge-file stride sampling was used
}

// Phase is a temporal interval during which a distinct cluster set was loading.
type Phase struct {
	Start, End time.Duration
	ClusterIDs []int
}

// Sampler continuously snapshots a file's page residency at a fixed interval.
type Sampler struct {
	path     string
	interval time.Duration
	baseline []bool
	stop     chan struct{}
	done     chan struct{}
	snaps    []Snapshot
	mu       sync.Mutex
	latest   Snapshot
}

// New creates a Sampler for path. baseline is the before-snapshot used to
// compute NewPages on each captured Snapshot. Call Start to begin sampling.
func New(path string, interval time.Duration, baseline []bool) *Sampler {
	return &Sampler{
		path:     path,
		interval: interval,
		baseline: baseline,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the background sampling goroutine.
func (s *Sampler) Start() {
	go s.run()
}

// Stop signals the sampler to finish and waits for it to drain.
func (s *Sampler) Stop() []Snapshot {
	close(s.stop)
	<-s.done
	return s.snaps
}

// Latest returns the most recent snapshot. Safe to call from any goroutine.
func (s *Sampler) Latest() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

func (s *Sampler) run() {
	defer close(s.done)
	t0 := time.Now()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			s.capture(time.Since(t0))
			return
		case t := <-ticker.C:
			s.capture(t.Sub(t0))
		}
	}
}

func (s *Sampler) capture(elapsed time.Duration) {
	res, size, sampled, err := mincore.Residency(s.path)
	if err != nil {
		return
	}
	snap := Snapshot{
		At:        elapsed,
		NewPages:  countNewPages(res, s.baseline),
		Residency: res,
		FileSize:  size,
		Sampled:   sampled,
	}
	s.mu.Lock()
	s.latest = snap
	s.mu.Unlock()
	s.snaps = append(s.snaps, snap)
}

func countNewPages(current, baseline []bool) int64 {
	var n int64
	for i, in := range current {
		if in && (i >= len(baseline) || !baseline[i]) {
			n++
		}
	}
	return n
}
