/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
