package sampler

import (
	"time"

	"github.com/erigontech/erigon/cmd/pgwatch/internal/mincore"
)

// Snapshot is a single mincore reading taken at a point in time.
type Snapshot struct {
	At        time.Duration // elapsed since sampling started
	Residency []bool
	FileSize  int64
	Sampled   bool // true if huge-file stride sampling was used
}

// Phase is a temporal interval during which a distinct cluster set was loading.
type Phase struct {
	Start, End time.Duration
	ClusterIDs []int // indices into the cluster slice for that file
}

// Sampler continuously snapshots a file's page residency at a fixed interval
// while a command runs.
type Sampler struct {
	path     string
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	snaps    []Snapshot
	latest   Snapshot
}

// New creates a Sampler for path. Call Start to begin sampling.
func New(path string, interval time.Duration) *Sampler {
	return &Sampler{
		path:     path,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the background sampling goroutine and records t0.
func (s *Sampler) Start() {
	go s.run()
}

// Stop signals the sampler to finish and waits for it to drain.
func (s *Sampler) Stop() []Snapshot {
	close(s.stop)
	<-s.done
	return s.snaps
}

func (s *Sampler) run() {
	defer close(s.done)
	t0 := time.Now()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			// one final snapshot
			s.capture(time.Since(t0))
			return
		case t := <-ticker.C:
			s.capture(t.Sub(t0))
		}
	}
}

// Latest returns the most recent snapshot without stopping the sampler.
// Safe to call from another goroutine while sampling is running.
func (s *Sampler) Latest() Snapshot { return s.latest }

func (s *Sampler) capture(elapsed time.Duration) {
	res, size, sampled, err := mincore.Residency(s.path)
	if err != nil {
		return
	}
	snap := Snapshot{
		At:        elapsed,
		Residency: res,
		FileSize:  size,
		Sampled:   sampled,
	}
	s.latest = snap
	s.snaps = append(s.snaps, snap)
}
