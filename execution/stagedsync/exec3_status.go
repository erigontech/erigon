package stagedsync

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon/core/state"
)

type ExecutionStat struct {
	TxIdx       int
	Incarnation int
	Duration    time.Duration
}

// Find the longest execution path in the DAG
func LongestPath(d *state.DAG, stats map[int]ExecutionStat) ([]int, uint64) {
	prev := make(map[int]int, len(d.GetVertices()))

	for i := 0; i < len(d.GetVertices()); i++ {
		prev[i] = -1
	}

	pathWeights := make(map[int]uint64, len(d.GetVertices()))

	maxPath := 0
	maxPathWeight := uint64(0)

	idxToId := make(map[int]string, len(d.GetVertices()))

	for k, i := range d.GetVertices() {
		idxToId[i.(int)] = k
	}

	for i := 0; i < len(idxToId); i++ {
		parents, _ := d.GetParents(idxToId[i])

		if len(parents) > 0 {
			for _, p := range parents {
				weight := pathWeights[p.(int)] + uint64(stats[i].Duration)
				if weight > pathWeights[i] {
					pathWeights[i] = weight
					prev[i] = p.(int)
				}
			}
		} else {
			pathWeights[i] = uint64(stats[i].Duration)
		}

		if pathWeights[i] > maxPathWeight {
			maxPath = i
			maxPathWeight = pathWeights[i]
		}
	}

	path := make([]int, 0)
	for i := maxPath; i != -1; i = prev[i] {
		path = append(path, i)
	}

	// Reverse the path so the transactions are in the ascending order
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return path, maxPathWeight
}

func ReportDAG(d *state.DAG, stats map[int]ExecutionStat, out func(string)) {
	longestPath, weight := LongestPath(d, stats)

	serialWeight := uint64(0)

	for i := 0; i < len(d.GetVertices()); i++ {
		serialWeight += uint64(stats[i].Duration)
	}

	makeStrs := func(ints []int) (ret []string) {
		for _, v := range ints {
			ret = append(ret, strconv.Itoa(v))
		}

		return
	}

	out("Longest execution path:")
	out(fmt.Sprintf("(%v) %v", len(longestPath), strings.Join(makeStrs(longestPath), "->")))

	out(fmt.Sprintf("Longest path ideal execution time: %v of %v (serial total), %v%%", time.Duration(weight),
		time.Duration(serialWeight), fmt.Sprintf("%.1f", float64(weight)*100.0/float64(serialWeight))))
}

type execStatusList struct {
	pending    []int
	inProgress []int
	complete   []int
	dependency map[int]map[int]bool
	blocker    map[int]map[int]bool
}

func insertInList(l []int, v int) []int {
	if len(l) == 0 || v > l[len(l)-1] {
		return append(l, v)
	} else {
		x := sort.SearchInts(l, v)
		if x < len(l) && l[x] == v {
			// already in list
			return l
		}
		a := append(l[:x+1], l[x:]...)
		a[x] = v
		return a
	}
}

func (m *execStatusList) takeNextPending() int {
	if len(m.pending) == 0 {
		return -1
	}

	x := m.pending[0]
	m.pending = m.pending[1:]
	m.inProgress = insertInList(m.inProgress, x)

	return x
}

func hasNoGap(l []int) bool {
	return l[0]+len(l) == l[len(l)-1]+1
}

func (m execStatusList) maxComplete() int {
	if len(m.complete) == 0 || m.complete[0] != 0 {
		return -1
	} else if m.complete[len(m.complete)-1] == len(m.complete)-1 {
		return m.complete[len(m.complete)-1]
	} else {
		for i := len(m.complete) - 2; i >= 0; i-- {
			if hasNoGap(m.complete[:i+1]) {
				return m.complete[i]
			}
		}
	}

	return -1
}

func (m *execStatusList) pushPending(tx int) {
	m.pending = insertInList(m.pending, tx)
}

func removeFromList(l []int, v int, expect bool) []int {
	x := sort.SearchInts(l, v)
	if x == -1 || l[x] != v {
		if expect {
			panic(errors.New("should not happen - element expected in list"))
		}

		return l
	}

	switch x {
	case 0:
		return l[1:]
	case len(l) - 1:
		return l[:len(l)-1]
	default:
		return append(l[:x], l[x+1:]...)
	}
}

func (m *execStatusList) markComplete(tx int) {
	m.inProgress = removeFromList(m.inProgress, tx, true)
	m.complete = insertInList(m.complete, tx)
}

func (m *execStatusList) minPending() int {
	if len(m.pending) == 0 {
		return -1
	} else {
		return m.pending[0]
	}
}

func (m *execStatusList) countComplete() int {
	return len(m.complete)
}

func (m *execStatusList) addDependency(blocker int, dependent int) bool {
	if blocker < 0 || blocker >= dependent {
		return false
	}

	curblockers := m.blocker[dependent]

	if m.checkComplete(blocker) {
		// Blocker has already completed
		delete(curblockers, blocker)
		return len(curblockers) > 0
	}

	if _, ok := m.dependency[blocker]; !ok {
		if m.dependency == nil {
			m.dependency = map[int]map[int]bool{
				blocker: {},
			}
		} else {
			m.dependency[blocker] = map[int]bool{}
		}
	}

	m.dependency[blocker][dependent] = true

	if curblockers == nil {
		curblockers = map[int]bool{}
		if m.blocker == nil {
			m.blocker = map[int]map[int]bool{
				dependent: curblockers,
			}
		} else {
			m.blocker[dependent] = curblockers
		}
	}

	curblockers[blocker] = true

	return true
}

func (m *execStatusList) isBlocked(tx int) bool {
	return len(m.blocker[tx]) > 0
}

func (m *execStatusList) removeDependency(tx int) {
	if deps, ok := m.dependency[tx]; ok && len(deps) > 0 {
		for k := range deps {
			delete(m.blocker[k], tx)

			if len(m.blocker[k]) == 0 {
				if !m.checkComplete(k) && !m.checkPending(k) && !m.checkInProgress(k) {
					m.pushPending(k)
				}
			}
		}

		delete(m.dependency, tx)
	}
}

func (m *execStatusList) clearInProgress(tx int) {
	m.inProgress = removeFromList(m.inProgress, tx, true)
}

func (m *execStatusList) checkInProgress(tx int) bool {
	x := sort.SearchInts(m.inProgress, tx)
	if x < len(m.inProgress) && m.inProgress[x] == tx {
		return true
	}

	return false
}

func (m *execStatusList) checkPending(tx int) bool {
	x := sort.SearchInts(m.pending, tx)
	if x < len(m.pending) && m.pending[x] == tx {
		return true
	}

	return false
}

func (m *execStatusList) checkComplete(tx int) bool {
	x := sort.SearchInts(m.complete, tx)
	if x < len(m.complete) && m.complete[x] == tx {
		return true
	}

	return false
}

// getRevalidationRange: this range will be all tasks from tx (inclusive) that are not currently in progress up to the
//
//	'all complete' limit
func (m *execStatusList) getRevalidationRange(txFrom int) (ret []int) {
	max := m.maxComplete() // haven't learned to trust compilers :)
	for x := txFrom; x <= max; x++ {
		if !m.checkInProgress(x) {
			ret = append(ret, x)
		}
	}

	return
}

func (m *execStatusList) pushPendingSet(set []int) {
	for _, v := range set {
		if m.checkComplete(v) {
			m.clearComplete(v)
		}

		m.pushPending(v)
	}
}

func (m *execStatusList) clearComplete(tx int) {
	m.complete = removeFromList(m.complete, tx, false)
}

func (m *execStatusList) clearPending(tx int) {
	m.pending = removeFromList(m.pending, tx, false)
}
