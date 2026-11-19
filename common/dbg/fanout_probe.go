package dbg

import (
	"strconv"
	"sync"
	"time"
)

// Fan-out probe marks, not for merge: the first and last time of a named event per block.
var (
	probeMu    sync.Mutex
	probeFirst = map[string]int64{}
	probeLast  = map[string]int64{}
)

func ProbeMark(name string, block uint64) {
	k := name + "/" + strconv.FormatUint(block, 10)
	now := time.Now().UnixNano()
	probeMu.Lock()
	if _, ok := probeFirst[k]; !ok {
		probeFirst[k] = now
	}
	probeLast[k] = now
	probeMu.Unlock()
}

// ProbeTake returns and forgets the first and last time of name at block, 0 when not marked.
func ProbeTake(name string, block uint64) (first, last int64) {
	k := name + "/" + strconv.FormatUint(block, 10)
	probeMu.Lock()
	first, last = probeFirst[k], probeLast[k]
	delete(probeFirst, k)
	delete(probeLast, k)
	probeMu.Unlock()
	return first, last
}
