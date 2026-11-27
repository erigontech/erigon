package stagedsync

import (
	"bytes"
	"sync/atomic"
	"time"
)

const bpSlowRead = 100 * time.Microsecond

// bpReads counts commitment branch reads and the subset whose key the previous
// call's prefetcher already held (same = byte-equal record).
type bpReads struct {
	n, ns, slow, prev, prevNs, prevSlow, prevSame atomic.Int64
}

func (s *bpReads) note(key, data []byte, d time.Duration) {
	s.n.Add(1)
	s.ns.Add(int64(d))
	slow := d >= bpSlowRead
	if slow {
		s.slow.Add(1)
	}
	prev := bpPrev.Load()
	if prev == nil {
		return
	}
	r, ok := prev.lookup(key)
	if !ok {
		return
	}
	s.prev.Add(1)
	s.prevNs.Add(int64(d))
	if slow {
		s.prevSlow.Add(1)
	}
	if bytes.Equal(r.data, data) {
		s.prevSame.Add(1)
	}
}

func (s *bpReads) args(p string) []any {
	return []any{p + "N", s.n.Load(), p + "Us", s.ns.Load() / 1000, p + "Slow", s.slow.Load(),
		p + "PrevN", s.prev.Load(), p + "PrevUs", s.prevNs.Load() / 1000, p + "PrevSlow", s.prevSlow.Load(), p + "PrevSame", s.prevSame.Load()}
}

type bpProbe struct {
	added             atomic.Int64
	worker, fold      bpReads
	foldHit, foldMem  atomic.Int64
	consNs, ccConsNs  int64
	foldQueue, foldWN int64
	foldSeen          atomic.Bool
}

// bpPrev is the previous call's prefetcher, kept only to classify this call's reads.
var bpPrev atomic.Pointer[branchPrefetcher]
