package dataflow

import (
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
)

var BlockBodyDownloadStates *States = NewStates(64*1024)

const (
	BlockBodyCleared = iota
	BlockBodyExpired
	BlockBodyRequested
	BlockBodyReceived
)

type SnapshotItem struct {
	id    uint64
	state byte
}

type States struct {
	lock         sync.Mutex
	window       int
	ids          []uint64
	ticks        []int64
	states       []byte
	snapshot     *btree.BTreeG[SnapshotItem]
	lastTick	 int64
	snapshotTick int64
	idx          int
}

func NewStates(window int) *States {
	s := &States{
		window: window,
		ids:    make([]uint64, window),
		ticks: make([]int64, window),
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
	s.lastTick++
	i := s.idx
	s.idx++
	s.ids[i] = id
	s.ticks[i] = s.lastTick
	s.states[i] = state
}

func (s *States) makeSnapshot() {
	newSnapshot := map[uint64]byte{}
	// snapshotTime is now time of the latest change
	s.snapshotTick = s.ticks[s.idx-1]
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

func (s *States) ChangesSince(tick int64, w io.Writer) {
	fmt.Fprintf(w, "%d\n", s.snapshotTick)
	if tick <= s.snapshotTick {
		// Include snapshot
		s.snapshot.Ascend(func(a SnapshotItem) bool {
			fmt.Fprintf(w, "%d,%d\n", a.id, a.state)
			return true
		})
	}
	fmt.Fprintf(w, "\n")
	for i := 0; i < s.idx; i++ {
		if s.ticks[i] < tick {
			continue
		}
		fmt.Fprintf(w, "%d,%d,%d\n", s.ids[i], s.states[i], s.ticks[i])
	}
}
