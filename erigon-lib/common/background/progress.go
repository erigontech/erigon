// Copyright 2021 The Erigon Authors
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

package background

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	btree2 "github.com/tidwall/btree"
)

// Progress - tracks background job progress
type Progress struct {
	Name             atomic.Pointer[string]
	Processed, Total atomic.Uint64
	i                int
}

func (p *Progress) percent() int {
	return int(
		(float64(p.Processed.Load()) / float64(p.Total.Load())) * 100,
	)
}

// ProgressSet - tracks multiple background job progress
type ProgressSet struct {
	list *btree2.Map[int, *Progress]
	i    int
	lock sync.RWMutex
}

func NewProgressSet() *ProgressSet {
	return &ProgressSet{list: btree2.NewMap[int, *Progress](128)}
}
func (s *ProgressSet) AddNew(fName string, total uint64) *Progress {
	p := &Progress{}
	p.Name.Store(&fName)
	p.Total.Store(total)
	s.Add(p)
	return p
}
func (s *ProgressSet) Add(p *Progress) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.i++
	p.i = s.i
	s.list.Set(p.i, p)
}

func (s *ProgressSet) Delete(p *Progress) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.list.Delete(p.i)
}
func (s *ProgressSet) Has() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.list.Len() > 0
}

func (s *ProgressSet) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var sb strings.Builder
	var i int
	s.list.Scan(func(_ int, p *Progress) bool {
		if p == nil {
			return true
		}
		namePtr := p.Name.Load()
		if namePtr == nil {
			return true
		}
		sb.WriteString(fmt.Sprintf("%s=%d%%", *namePtr, p.percent()))
		i++
		if i != s.list.Len() {
			sb.WriteString(", ")
		}
		return true
	})
	return sb.String()
}

func (s *ProgressSet) DiagnosticsData() map[string]int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var arr = make(map[string]int, s.list.Len())
	s.list.Scan(func(_ int, p *Progress) bool {
		if p == nil {
			return true
		}
		namePtr := p.Name.Load()
		if namePtr == nil {
			return true
		}
		arr[*namePtr] = p.percent()
		return true
	})
	return arr
}
