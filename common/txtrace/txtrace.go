// Package txtrace records where ONE sender's transactions reach in the pipeline, per nonce.
//
// ⚠ IT EXISTS BECAUSE AGGREGATES CANNOT ANSWER A PER-TRANSACTION QUESTION. "4,800 submitted, 4,154
// held, 544 canonical, so 102 were removed as mined without mining" subtracts one sender's counts
// from a pool-wide tally across every sender; "the lowest nonce offered is 652" was the minimum of
// per-round maxima. Both read as findings and neither was one. A question like "where is nonce 544"
// is a lookup, not arithmetic.
//
// ⚠ AND IT MUST NOT CHANGE WHAT IT MEASURES. The first version logged inline on the drain's hot
// path, and that alone decided the outcome: with it on, 4 of 4 bursts drained; with it off, 1 of 3,
// the rest leaving thousands pending. An instrument that suppresses the fault is worse than none,
// so the hot path now does an append to a preallocated array and nothing else — no formatting, no
// allocation, no lock contention beyond one atomic increment. A background writer drains the ring
// to a file, because the nodes under test are killed with -9 and an exit hook would never run.
//
// Scoped to one sender by COCOON_TRACE_SENDER, written to COCOON_TRACE_FILE. Off unless both are
// set; when off, Record is an atomic load and a branch.
package txtrace

import (
	"bufio"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Stage is a point in the pipeline a transaction can reach. Small integer, not a string: the hot
// path stores it, and a string would mean an interface box or a copy per record.
type Stage uint8

const (
	PoolAdmitted Stage = iota
	PoolExit
	FedToDAG
	DAGOrdered
	ExecOffered
	ExecFiltered
	ExecFuture
	ExecStale
	ExecCommitted
)

var stageName = [...]string{
	"pool-admitted", "pool-exit", "fed-to-dag", "dag-ordered",
	"exec-offered", "exec-filter-kept", "exec-future", "exec-stale", "exec-committed",
}

// String names the stage for the dump.
func (s Stage) String() string {
	if int(s) < len(stageName) {
		return stageName[s]
	}
	return "stage?"
}

type rec struct {
	at    int64
	nonce uint64
	aux   uint64
	hash  [32]byte
	stage Stage
	kind  uint8 // aux meaning: 0 none, 1 block, 2 expected nonce, 3 discard reason
}

const ringSize = 1 << 20 // ~64MB of records; a 4,800 burst produces well under 100k

var (
	on     atomic.Bool
	target string
	ring   []rec
	next   atomic.Int64
)

func init() {
	addr := strings.ToLower(strings.TrimSpace(os.Getenv("COCOON_TRACE_SENDER")))
	path := strings.TrimSpace(os.Getenv("COCOON_TRACE_FILE"))
	if addr == "" || path == "" {
		return
	}
	target = addr
	ring = make([]rec, ringSize)
	on.Store(true)
	go writer(path)
}

// On reports whether tracing is enabled. Check it before building any argument.
func On() bool { return on.Load() }

// Tracks reports whether this address is the one being traced.
func Tracks(addr string) bool {
	if !on.Load() {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(addr), target)
}

// Record appends one row. Everything expensive — formatting, the file — happens in the writer.
func Record(s Stage, nonce uint64, hash [32]byte, aux uint64, kind uint8) {
	if !on.Load() {
		return
	}
	if nonce != 0 {
		known.Store(hash, struct{}{})
	}
	i := next.Add(1) - 1
	if i >= ringSize {
		return // full: drop rather than overwrite, so what IS recorded stays ordered and true
	}
	ring[i] = rec{at: time.Now().UnixMicro(), nonce: nonce, aux: aux, hash: hash, stage: s, kind: kind}
}

var auxName = [...]string{"", "block", "expected", "reason"}

func writer(path string) {
	f, err := os.Create(path)
	if err != nil {
		on.Store(false)
		return
	}
	w := bufio.NewWriterSize(f, 1<<20)
	written := int64(0)
	for {
		time.Sleep(2 * time.Second)
		end := next.Load()
		if end > ringSize {
			end = ringSize
		}
		for ; written < end; written++ {
			r := &ring[written]
			w.WriteString(strconv.FormatInt(r.at, 10))
			w.WriteByte(' ')
			w.WriteString(r.stage.String())
			w.WriteString(" nonce=")
			w.WriteString(strconv.FormatUint(r.nonce, 10))
			w.WriteString(" hash=0x")
			w.WriteString(hex.EncodeToString(r.hash[:]))
			if r.kind != 0 && int(r.kind) < len(auxName) {
				w.WriteByte(' ')
				w.WriteString(auxName[r.kind])
				w.WriteByte('=')
				w.WriteString(strconv.FormatUint(r.aux, 10))
			}
			w.WriteByte('\n')
		}
		w.Flush()
	}
}

// known holds the hashes seen for the traced sender, so a stage that has only a hash can still be
// filtered to that sender.
//
// ⚠ WITHOUT IT, exec-committed RECORDED EVERY SENDER. Every other stage is gated on Tracks(), but
// the commit site has no sender — it iterates RoundKeptHashes — so it recorded the whole round.
// That made "committed rows" exceed "filter-kept rows" and turned a comparison meant to expose
// rolled-back transactions into noise.
var known sync.Map // [32]byte -> struct{}

// Known reports whether this hash belongs to the traced sender, as learned from a stage that knew
// the nonce. False before the pool has admitted it, which is correct: nothing earlier can be ours.
func Known(h [32]byte) bool {
	if !on.Load() {
		return false
	}
	_, ok := known.Load(h)
	return ok
}
