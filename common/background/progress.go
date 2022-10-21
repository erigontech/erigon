package background

import (
	"fmt"
	"strings"
	"sync"

	"github.com/google/btree"
	"go.uber.org/atomic"
)

// Progress - tracks background job progress
type Progress struct {
	Name             atomic.String
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
	b    *btree.BTreeG[*Progress]
	i    int
	lock sync.RWMutex
}

func NewProgressSet() *ProgressSet {
	return &ProgressSet{b: btree.NewG(4, func(i, j *Progress) bool { return i.i < j.i })}
}

func (s *ProgressSet) Add(p *Progress) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.i++
	p.i = s.i
	s.b.ReplaceOrInsert(p)
}

func (s *ProgressSet) Delete(p *Progress) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.b.Delete(p)
}

func (s *ProgressSet) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var sb strings.Builder
	var i int
	s.b.Ascend(func(p *Progress) bool {
		sb.WriteString(fmt.Sprintf("%s=%d%%", p.Name.Load(), p.percent()))
		i++
		if i != s.b.Len() {
			sb.WriteString(", ")
		}
		return true
	})
	return sb.String()
}
