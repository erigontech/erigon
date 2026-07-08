package stagedsync

import (
	"errors"
	"sort"
	"time"
)

type ExecutionStat struct {
	TxIdx       int
	Incarnation int
	Duration    time.Duration
}

// execStatusList tracks per-tx scheduling state for one block. pending/deferred
// stay sorted []int (populated in-order, popped from the front). complete and
// inProgress use dense []bool membership because completions arrive out of tx
// order across workers — a sorted-slice insert/delete there is O(n) per event,
// O(n²) over a block, which dominates high-tx-count (e.g. ether_transfers) blocks.
type execStatusList struct {
	pending    []int
	deferred   []int // txs whose retry waits on a directed-delay predicate
	inProg     []bool
	comp       []bool
	dependency map[int]map[int]bool
	blocker    map[int]map[int]bool
	inProgCnt  int
	compCnt    int
	// completeUpTo-1 == maxComplete: the contiguous-from-zero complete prefix, O(1).
	completeUpTo int
}

func (m *execStatusList) ensureLen(tx int) {
	if tx < len(m.comp) {
		return
	}
	grow := tx + 1 - len(m.comp)
	m.comp = append(m.comp, make([]bool, grow)...)
	m.inProg = append(m.inProg, make([]bool, grow)...)
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
	m.ensureLen(x)
	if !m.inProg[x] {
		m.inProg[x] = true
		m.inProgCnt++
	}

	return x
}

func (m execStatusList) maxComplete() int {
	return m.completeUpTo - 1
}

func (m *execStatusList) pushPending(tx int) {
	m.ensureLen(tx)
	m.pending = insertInList(m.pending, tx)
}

// pushDeferred parks a tx that hit ErrDependency with no effective blocker
// (or was invalidated mid-flight). Immediate re-dispatch re-enters the
// race; drainDeferredIfReady gates retry on a directed-delay predicate.
func (m *execStatusList) pushDeferred(tx int) {
	m.deferred = insertInList(m.deferred, tx)
}

// drainDeferred unconditionally moves deferred → pending. Forward-progress
// safety net when no workers are in flight.
func (m *execStatusList) drainDeferred() {
	for _, tx := range m.deferred {
		m.pending = insertInList(m.pending, tx)
	}
	m.deferred = m.deferred[:0]
}

func (m *execStatusList) drainDeferredIfReady(ready func(tx int) bool) {
	if len(m.deferred) == 0 {
		return
	}
	kept := m.deferred[:0]
	for _, tx := range m.deferred {
		if ready(tx) {
			m.pending = insertInList(m.pending, tx)
		} else {
			kept = append(kept, tx)
		}
	}
	m.deferred = kept
}

func (m *execStatusList) inProgressCount() int { return m.inProgCnt }

// minInProgress returns the lowest in-progress tx index, or -1 if empty.
// Cold path: only reached while deferred tasks exist (conflict workloads).
func (m *execStatusList) minInProgress() int {
	if m.inProgCnt == 0 {
		return -1
	}
	for i, v := range m.inProg {
		if v {
			return i
		}
	}
	return -1
}

func removeFromList(l []int, v int, expect bool) []int {
	x := sort.SearchInts(l, v)
	if x == -1 || x >= len(l) || l[x] != v {
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
	m.ensureLen(tx)
	if m.inProg[tx] {
		m.inProg[tx] = false
		m.inProgCnt--
	}
	if !m.comp[tx] {
		m.comp[tx] = true
		m.compCnt++
	}
	if tx == m.completeUpTo {
		for m.completeUpTo < len(m.comp) && m.comp[m.completeUpTo] {
			m.completeUpTo++
		}
	}
}

func (m *execStatusList) minPending() int {
	if len(m.pending) == 0 {
		return -1
	} else {
		return m.pending[0]
	}
}

func (m *execStatusList) countComplete() int {
	return m.compCnt
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
	if tx < len(m.inProg) && m.inProg[tx] {
		m.inProg[tx] = false
		m.inProgCnt--
	} else if tx >= len(m.inProg) {
		panic(errors.New("should not happen - element expected in list"))
	}
}

func (m *execStatusList) checkInProgress(tx int) bool {
	return tx >= 0 && tx < len(m.inProg) && m.inProg[tx]
}

func (m *execStatusList) checkPending(tx int) bool {
	x := sort.SearchInts(m.pending, tx)
	if x < len(m.pending) && m.pending[x] == tx {
		return true
	}

	return false
}

func (m *execStatusList) checkComplete(tx int) bool {
	return tx >= 0 && tx < len(m.comp) && m.comp[tx]
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
	if tx >= 0 && tx < len(m.comp) && m.comp[tx] {
		m.comp[tx] = false
		m.compCnt--
	}
	if tx < m.completeUpTo {
		m.completeUpTo = tx
	}
}

func (m *execStatusList) clearPending(tx int) {
	m.pending = removeFromList(m.pending, tx, false)
}

// completeList / inProgressList reconstruct the sorted index slice (test helpers).
func (m *execStatusList) completeList() []int {
	out := make([]int, 0, m.compCnt)
	for i, v := range m.comp {
		if v {
			out = append(out, i)
		}
	}
	return out
}

func (m *execStatusList) inProgressList() []int {
	out := make([]int, 0, m.inProgCnt)
	for i, v := range m.inProg {
		if v {
			out = append(out, i)
		}
	}
	return out
}
