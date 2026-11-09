package stagedsync

import (
	"context"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// foldProbe attributes one block's commitment fold; only the calculator goroutine touches it.
type foldProbe struct {
	firstUpdate time.Time
	foldEnd     time.Time

	idle       time.Duration
	accumulate time.Duration
	prefetch   time.Duration
	emit       time.Duration
	flush      time.Duration
	trie       time.Duration
	fold       commitment.FoldTiming
}

func addSince(d *time.Duration, start time.Time) { *d += time.Since(start) }

func (p *foldProbe) markFirstUpdate() {
	if p.firstUpdate.IsZero() {
		p.firstUpdate = time.Now()
	}
}

func (p *foldProbe) markFoldEnd() { p.foldEnd = time.Now() }

func (p *foldProbe) addIdle(waitStart time.Time) {
	if !p.firstUpdate.IsZero() && p.foldEnd.IsZero() {
		addSince(&p.idle, waitStart)
	}
}

func (cc *commitmentCalculator) computeTrie(ctx context.Context, t commitTarget, diff *kv.DomainDiff) ([]byte, error) {
	defer addSince(&cc.probe.trie, time.Now())
	return cc.doms.GetCommitmentContext().ComputeCommitmentWithDiff(ctx, cc.roTx, true, t.blockNum, t.lastTxNum, cc.logPrefix, cc.recordProgress, diff)
}

func (cc *commitmentCalculator) logFoldProbe() {
	p := &cc.probe
	if p.foldEnd.IsZero() {
		return
	}
	r := cc.lastBlockResult
	var afterExec time.Duration
	if !r.execEndedAt.IsZero() {
		afterExec = p.foldEnd.Sub(r.execEndedAt)
	}
	total := p.foldEnd.Sub(p.firstUpdate)
	if cc.logger != nil {
		cc.logger.Info("[np-phase] fold", "blk", r.BlockNum,
			"total", total, "parallel", p.fold.Parallel, "serial", total-p.fold.Parallel, "idle", p.idle,
			"forks", p.fold.Forks, "helpers", p.fold.Helpers,
			"accumulate", p.accumulate, "prefetch", p.prefetch, "emit", p.emit, "flush", p.flush,
			"build", p.fold.Build, "walkSerial", p.fold.Walk-p.fold.Parallel, "rootFold", p.fold.RootFold,
			"trieOther", p.trie-p.fold.Build-p.fold.Walk-p.fold.RootFold,
			"other", total-p.idle-p.accumulate-p.prefetch-p.emit-p.flush-p.trie,
			"afterExec", afterExec)
	}
	*p = foldProbe{}
}
