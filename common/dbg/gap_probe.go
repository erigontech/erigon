package dbg

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Gap probe, not for merge: splits the delay between the head and a log written on the same
// connection, for logs that match a filter with addresses.

type gapArrival struct {
	ns  int64
	seq int64
}

type gapRec struct {
	arr, seq, wake, write, head, gap int64
	logBeforeHead                    bool
}

var (
	gapHeaderNs  atomic.Int64
	gapLogSeq    atomic.Int64
	gapLogFirst  atomic.Int64
	gapLogLast   atomic.Int64
	gapHeadFirst atomic.Int64
	gapHeadLast  atomic.Int64
	gapArr       sync.Map // log pointer -> gapArrival
	gapHeadOf    sync.Map // connection -> head write time
	gapMu        sync.Mutex
	gapRecs      []gapRec
)

// GapHeader closes the previous block and starts a new one. It returns the summary of the
// previous block as key/value pairs, nil when there was none.
func GapHeader() []any {
	now := time.Now().UnixNano()
	h0 := gapHeaderNs.Swap(now)
	total := gapLogSeq.Swap(0)
	lf, ll := gapLogFirst.Swap(0), gapLogLast.Swap(0)
	hf, hl := gapHeadFirst.Swap(0), gapHeadLast.Swap(0)
	gapMu.Lock()
	recs := gapRecs
	gapRecs = nil
	gapMu.Unlock()
	gapArr.Clear()
	if h0 == 0 {
		return nil
	}
	ms := func(v int64) float64 { return float64(v) / 1e6 }
	rel := func(v int64) float64 {
		if v == 0 {
			return -1
		}
		return ms(v - h0)
	}
	q := func(get func(r gapRec) int64, p float64) float64 {
		if len(recs) == 0 {
			return -1
		}
		v := make([]int64, len(recs))
		for i, r := range recs {
			v[i] = get(r)
		}
		slices.Sort(v)
		return ms(v[min(len(v)-1, int(p*float64(len(v))))])
	}
	before := 0
	for _, r := range recs {
		if r.logBeforeHead {
			before++
		}
	}
	seqFrac := func(p float64) float64 {
		if total == 0 {
			return -1
		}
		return q(func(r gapRec) int64 { return r.seq * 1e6 / total }, p)
	}
	return []any{"logs", total, "logFirst", rel(lf), "logLast", rel(ll), "headFirst", rel(hf), "headLast", rel(hl),
		"dapp", len(recs), "arrBeforeHead", before,
		"arr_p50", q(func(r gapRec) int64 { return r.arr }, .5), "arr_p90", q(func(r gapRec) int64 { return r.arr }, .9),
		"seqFrac_p50", seqFrac(.5),
		"wake_p50", q(func(r gapRec) int64 { return r.wake }, .5), "wake_p90", q(func(r gapRec) int64 { return r.wake }, .9),
		"write_p50", q(func(r gapRec) int64 { return r.write }, .5), "write_p90", q(func(r gapRec) int64 { return r.write }, .9),
		"head_p50", q(func(r gapRec) int64 { return r.head }, .5), "head_p90", q(func(r gapRec) int64 { return r.head }, .9),
		"gap_p50", q(func(r gapRec) int64 { return r.gap }, .5), "gap_p90", q(func(r gapRec) int64 { return r.gap }, .9)}
}

// GapLogArrived marks a log reaching the rpc filters and returns its arrival.
func GapLogArrived() (ns, seq int64) {
	ns = time.Now().UnixNano()
	seq = gapLogSeq.Add(1)
	gapLogFirst.CompareAndSwap(0, ns)
	gapLogLast.Store(ns)
	return ns, seq
}

// GapLogMatched records the arrival of a log that matched a filter with addresses.
func GapLogMatched(lg any, ns, seq int64) {
	gapArr.LoadOrStore(lg, gapArrival{ns, seq})
}

// GapHeadWritten records the time the head was written on conn.
func GapHeadWritten(conn any) {
	now := time.Now().UnixNano()
	gapHeadOf.Store(conn, now)
	gapHeadFirst.CompareAndSwap(0, now)
	for {
		l := gapHeadLast.Load()
		if now <= l || gapHeadLast.CompareAndSwap(l, now) {
			return
		}
	}
}

// GapLogsWritten records logs written on conn by a subscription with addresses.
func GapLogsWritten(conn any, logs []any, wakeNs int64) {
	now := time.Now().UnixNano()
	h0 := gapHeaderNs.Load()
	headNs := int64(0)
	if v, ok := gapHeadOf.Load(conn); ok {
		headNs = v.(int64)
	}
	if headNs < h0 {
		headNs = 0
	}
	for _, lg := range logs {
		v, ok := gapArr.Load(lg)
		if !ok {
			continue
		}
		a := v.(gapArrival)
		r := gapRec{arr: a.ns - h0, seq: a.seq, wake: wakeNs - a.ns, write: now - wakeNs}
		if headNs != 0 {
			r.head, r.gap, r.logBeforeHead = headNs-h0, now-headNs, a.ns < headNs
		} else {
			r.head, r.gap = -1e6, -1e6
		}
		gapMu.Lock()
		gapRecs = append(gapRecs, r)
		gapMu.Unlock()
	}
}
