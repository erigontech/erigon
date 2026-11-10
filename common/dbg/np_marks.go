package dbg

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

type npMark struct {
	name string
	at   time.Time
}

var (
	npMu    sync.Mutex
	npT0    time.Time
	npMarks []npMark
	npSums  = map[string]time.Duration{}

	NpStubBal = EnvBool("NP_STUB_BAL", false)
)

func NpAdd(name string, d time.Duration) {
	npMu.Lock()
	defer npMu.Unlock()
	npSums[name] += d
}

func NpStart() {
	npMu.Lock()
	defer npMu.Unlock()
	npT0 = time.Now()
	npMarks = npMarks[:0]
	clear(npSums)
}

func NpMark(name string) {
	now := time.Now()
	npMu.Lock()
	defer npMu.Unlock()
	if !npT0.IsZero() {
		npMarks = append(npMarks, npMark{name, now})
	}
}

func NpLine() string {
	now := time.Now()
	npMu.Lock()
	defer npMu.Unlock()
	slices.SortStableFunc(npMarks, func(a, b npMark) int { return a.at.Compare(b.at) })
	var b strings.Builder
	prev := npT0
	for _, m := range npMarks {
		fmt.Fprintf(&b, "%s=%.2f/%.2f ", m.name, float64(m.at.Sub(npT0).Microseconds())/1000, float64(m.at.Sub(prev).Microseconds())/1000)
		prev = m.at
	}
	fmt.Fprintf(&b, "end=%.2f", float64(now.Sub(npT0).Microseconds())/1000)
	for k, v := range npSums {
		fmt.Fprintf(&b, " sum-%s=%.2f", k, float64(v.Microseconds())/1000)
	}
	npT0 = time.Time{}
	return b.String()
}
