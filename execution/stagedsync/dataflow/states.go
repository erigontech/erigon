// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package dataflow

import (
	"sync"

	btree "github.com/anacrolix/btree"
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
	snapshot     *btree.Map[SnapshotItem, SnapshotItem]
	snapshotTick int
	idx          int
}

func NewStates(window int) *States {
	s := &States{
		window: window,
		ids:    make([]uint64, window),
		states: make([]byte, window),
		snapshot: func() *btree.Map[SnapshotItem, SnapshotItem] {
			m := btree.MakeMap[SnapshotItem, SnapshotItem](func(a, b SnapshotItem) int {
				if a.id < b.id {
					return -1
				}
				if a.id > b.id {
					return 1
				}
				return 0
			})
			return &m
		}(),
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
			item := SnapshotItem{id: id, state: state}
			s.snapshot.Upsert(item, item)
		}
	}
	s.idx = 0
}
