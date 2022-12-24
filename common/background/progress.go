package background

import (
	"fmt"
	"strings"
	"sync"

	btree2 "github.com/tidwall/btree"
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
	list *btree2.Map[int, *Progress]
	i    int
	lock sync.RWMutex
}

func NewProgressSet() *ProgressSet {
	return &ProgressSet{list: btree2.NewMap[int, *Progress](128)}
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

func (s *ProgressSet) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var sb strings.Builder
	var i int
	s.list.Scan(func(_ int, p *Progress) bool {
		sb.WriteString(fmt.Sprintf("%s=%d%%", p.Name.Load(), p.percent()))
		i++
		if i != s.list.Len() {
			sb.WriteString(", ")
		}
		return true
	})
	return sb.String()
}
