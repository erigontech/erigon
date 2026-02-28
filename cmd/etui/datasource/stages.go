package datasource

import (
	"sync"
	"time"

	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// SyncMetrics holds derived sync metrics computed from StagesInfo snapshots.
type SyncMetrics struct {
	HeadBlock     uint64  // highest block across all stages
	PipelineHead  uint64  // Finish stage progress — last block that completed the full pipeline
	TipLagSeconds int64   // seconds since last stage movement (0 = unknown)
	ImportRate    float64 // blocks per minute over 60-second window
	Status        SyncStatus
}

// SyncStatus represents the overall sync health.
type SyncStatus int

const (
	StatusUnknown SyncStatus = iota
	StatusInSync             // tip lag < 30s
	StatusSyncing            // stages are moving but lagging
	StatusStalled            // no stage movement for > 60s
)

// String returns a human-readable label for the status.
func (s SyncStatus) String() string {
	switch s {
	case StatusInSync:
		return "IN SYNC"
	case StatusSyncing:
		return "SYNCING"
	case StatusStalled:
		return "STALLED"
	default:
		return "UNKNOWN"
	}
}

// SyncTracker computes derived sync metrics from successive StagesInfo snapshots.
type SyncTracker struct {
	mu sync.Mutex

	updateCount  int // number of Update() calls received
	prevHead     uint64
	prevTime     time.Time
	lastMoveTime time.Time // last time any stage progressed

	// Ring buffer for 60-second import rate calculation
	samples    []rateSample
	sampleIdx  int
	sampleFull bool
}

type rateSample struct {
	block uint64
	ts    time.Time
}

const rateSamples = 12 // 12 samples × ~5s poll interval ≈ 60 seconds

// NewSyncTracker creates a new SyncTracker.
func NewSyncTracker() *SyncTracker {
	return &SyncTracker{
		samples: make([]rateSample, rateSamples),
	}
}

// Update computes sync metrics from the latest StagesInfo.
func (t *SyncTracker) Update(info *commands.StagesInfo) SyncMetrics {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	t.updateCount++
	m := SyncMetrics{}

	// Head block: max progress across all stages
	for _, sp := range info.StagesProgress {
		if sp.Progress > m.HeadBlock {
			m.HeadBlock = sp.Progress
		}
	}

	// Pipeline head: Finish stage progress (last block that completed the full pipeline)
	for _, sp := range info.StagesProgress {
		if sp.Stage == stages.Finish {
			m.PipelineHead = sp.Progress
			break
		}
	}

	// Detect stage movement
	if t.prevTime.IsZero() {
		// First update — initialise but do NOT set lastMoveTime to now,
		// so the first poll shows StatusUnknown instead of a false green.
		t.prevHead = m.HeadBlock
		t.prevTime = now
	} else if m.HeadBlock != t.prevHead {
		t.lastMoveTime = now
		t.prevHead = m.HeadBlock
		t.prevTime = now
	}

	// Tip lag: seconds since last stage movement
	if t.lastMoveTime.IsZero() {
		m.TipLagSeconds = -1 // no movement observed yet
	} else {
		m.TipLagSeconds = int64(now.Sub(t.lastMoveTime).Seconds())
	}

	// Import rate: blocks/minute from 60-second ring buffer
	t.samples[t.sampleIdx] = rateSample{block: m.HeadBlock, ts: now}
	t.sampleIdx = (t.sampleIdx + 1) % rateSamples
	if t.sampleIdx == 0 {
		t.sampleFull = true
	}

	m.ImportRate = t.calcRate(now)

	// Status determination — need at least 2 samples to judge
	if t.updateCount < 2 || t.lastMoveTime.IsZero() {
		m.Status = StatusUnknown
	} else {
		switch {
		case m.TipLagSeconds < 30:
			m.Status = StatusInSync
		case m.TipLagSeconds >= 60:
			m.Status = StatusStalled
		default:
			m.Status = StatusSyncing
		}
	}

	return m
}

// calcRate computes blocks/minute from the ring buffer.
func (t *SyncTracker) calcRate(now time.Time) float64 {
	var oldest rateSample

	if t.sampleFull {
		oldest = t.samples[t.sampleIdx] // oldest sample in full ring
	} else {
		oldest = t.samples[0] // first sample
	}

	if oldest.ts.IsZero() {
		return 0
	}

	elapsed := now.Sub(oldest.ts).Seconds()
	if elapsed < 1 {
		return 0
	}

	newest := t.samples[(t.sampleIdx-1+rateSamples)%rateSamples]
	if newest.block <= oldest.block {
		return 0
	}

	blocksPerSec := float64(newest.block-oldest.block) / elapsed
	return blocksPerSec * 60 // blocks per minute
}
