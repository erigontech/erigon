package dataflow

import (
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
)

var BlockBodyDownloadStates *States = NewStates(64 * 1024)
var HeaderDownloadStates *States = NewStates(64 * 1024)

const (
	BlockBodyCleared byte = iota
	BlockBodyExpired
	BlockBodyRequested
	BlockBodyReceived
	BlockBodyEvicted
	BlockBodySkipped // Delivery requested but was skipped due to limitation on the size of the response
	BlockBodyEmpty   // Identified as empty and no need to be requested
	BlockBodyPrefetched
	BlockBodyInDb
)

const (
	HeaderInvalidated byte = iota
	HeaderRequested
	HeaderSkeletonRequested
	HeaderRetryNotReady
	HeaderEmpty
	HeaderBad
	HeaderEvicted
	HeaderInserted
)

type SnapshotItem struct {
	id    uint64
	state byte
}

type States struct {
	lock         sync.Mutex
	window       int
	ids          []uint64
	states       []byte
	snapshot     *btree.BTreeG[SnapshotItem]
	snapshotTick int
	idx          int
}

func NewStates(window int) *States {
	s := &States{
		window: window,
		ids:    make([]uint64, window),
		states: make([]byte, window),
		snapshot: btree.NewG[SnapshotItem](16, func(a, b SnapshotItem) bool {
			return a.id < b.id
		}),
		idx: 0,
	}
	return s
}

func (s *States) AddChange(id uint64, state byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.idx >= s.window {
		s.makeSnapshot()
	}
	i := s.idx
	s.idx++
	s.ids[i] = id
	s.states[i] = state
}

func (s *States) makeSnapshot() {
	newSnapshot := map[uint64]byte{}
	// snapshotTime is now time of the latest change
	s.snapshotTick += s.idx
	// Proceed backwards
	for i := s.idx - 1; i >= 0; i-- {
		if _, ok := newSnapshot[s.ids[i]]; !ok {
			newSnapshot[s.ids[i]] = s.states[i]
		}
	}
	for id, state := range newSnapshot {
		if state == 0 {
			s.snapshot.Delete(SnapshotItem{id: id})
		} else {
			s.snapshot.ReplaceOrInsert(SnapshotItem{id: id, state: state})
		}
	}
	s.idx = 0
}

func (s *States) ChangesSince(startTick int, w io.Writer) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var startI int
	var tick int
	if startTick <= s.snapshotTick {
		// Include snapshot
		fmt.Fprintf(w, "snapshot %d\n", s.snapshotTick)
		s.snapshot.Ascend(func(a SnapshotItem) bool {
			fmt.Fprintf(w, "%d,%d\n", a.id, a.state)
			return true
		})
		tick = s.snapshotTick + 1
	} else {
		startI = startTick - s.snapshotTick
		tick = startTick
	}
	fmt.Fprintf(w, "changes %d\n", tick)
	for i := startI; i < s.idx; i++ {
		fmt.Fprintf(w, "%d,%d\n", s.ids[i], s.states[i])
	}
}
