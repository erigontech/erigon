package stagedsync

import (
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/exec"
)

// npConsProbe sums, per ExecV3 call, the wall time of executor construction and the
// per-worker task timing by the task's ordinal on its worker (0 = the worker's first task).
type npConsProbe struct {
	pool, reset, stop, rotx, rotxN atomic.Int64
	n, ns, gas                     [5]atomic.Int64
}

var npCons npConsProbe

func npOrdBucket(i int) int {
	switch {
	case i < 2:
		return i
	case i < 4:
		return 2
	case i < 8:
		return 3
	}
	return 4
}

func (p *npConsProbe) clear() {
	p.pool.Store(0)
	p.reset.Store(0)
	p.stop.Store(0)
	p.rotx.Store(0)
	p.rotxN.Store(0)
	for i := range p.n {
		p.n[i].Store(0)
		p.ns[i].Store(0)
		p.gas[i].Store(0)
	}
	exec.NpIbsNs.Store(0)
	exec.NpIbsN.Store(0)
}

func (p *npConsProbe) task(ord int, inc0 bool, d time.Duration, gas uint64) {
	if !inc0 {
		return
	}
	b := npOrdBucket(ord)
	p.n[b].Add(1)
	p.ns[b].Add(int64(d))
	p.gas[b].Add(int64(gas))
}

func (p *npConsProbe) log(logger log.Logger, blk uint64) {
	args := []any{"blk", blk, "poolUs", p.pool.Load() / 1000, "resetUs", p.reset.Load() / 1000, "stopUs", p.stop.Load() / 1000,
		"ibsUs", exec.NpIbsNs.Load() / 1000, "ibsN", exec.NpIbsN.Load(), "rotxUs", p.rotx.Load() / 1000, "rotxN", p.rotxN.Load()}
	for i, name := range []string{"o0", "o1", "o2_3", "o4_7", "o8"} {
		args = append(args, name, [3]int64{p.n[i].Load(), p.ns[i].Load() / 1000, p.gas[i].Load()})
	}
	logger.Info("[np-cons] exec", args...)
}
