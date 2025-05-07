package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
	"golang.org/x/sync/errgroup"
)

type ForkableAgg struct {
	db     kv.RoDB
	dirs   datadir.Dirs
	tmpdir string

	marked          []*Forkable[MarkedTxI]
	unmarked        []*Forkable[UnmarkedTxI]
	buffered        []*Forkable[BufferedTxI]
	alignedEntities []ForkableId

	dirtyFilesLock             sync.Mutex
	visibleFilesLock           sync.RWMutex
	visibleFilesMinimaxRootNum atomic.Uint64

	buildingFiles atomic.Bool
	//mergingFiles  atomic.Bool

	collateAndBuildWorkers int

	ctx       context.Context
	ctxCancel context.CancelFunc

	wg sync.WaitGroup

	ps *background.ProgressSet

	leakDetector *dbg.LeakDetector
	logger       log.Logger
}

func NewForkableAgg(ctx context.Context, dirs datadir.Dirs, db kv.RoDB, logger log.Logger) *ForkableAgg {
	ctx, ctxCancel := context.WithCancel(ctx)
	return &ForkableAgg{
		db:        db,
		dirs:      dirs,
		tmpdir:    dirs.Tmp,
		ctx:       ctx,
		ctxCancel: ctxCancel,

		leakDetector:           dbg.NewLeakDetector("forkable_agg", dbg.SlowTx()),
		logger:                 logger,
		collateAndBuildWorkers: 1,
		ps:                     background.NewProgressSet(),

		// marked:   ap.marked,
		// unmarked: ap.unmarked,
		// buffered: ap.buffered,
	}
}

func (r *ForkableAgg) RegisterMarkedForkable(ap *Forkable[MarkedTxI]) {
	r.marked = append(r.marked, ap)
	if !ap.unaligned {
		r.alignedEntities = append(r.alignedEntities, ap.a)
	}
}

func (r *ForkableAgg) RegisterUnmarkedForkable(ap *Forkable[UnmarkedTxI]) {
	r.unmarked = append(r.unmarked, ap)
	if !ap.unaligned {
		r.alignedEntities = append(r.alignedEntities, ap.a)
	}
}

func (r *ForkableAgg) RegisterBufferedForkable(ap *Forkable[BufferedTxI]) {
	r.buffered = append(r.buffered, ap)
	if !ap.unaligned {
		r.alignedEntities = append(r.alignedEntities, ap.a)
	}
}

// - "open folder"
// - close
// - build files
// merge files
// get index
// quick prune
// prune
// debug interface (files/db)
// - temporal agg interface

func (r *ForkableAgg) OpenFolder() error {
	r.dirtyFilesLock.Lock()
	defer r.dirtyFilesLock.Unlock()
	if err := r.openFolder(); err != nil {
		return err
	}

	return nil
}

// BuildFiles builds all snapshots (asynchronously) upto a given RootNum
func (r *ForkableAgg) BuildFiles(num RootNum, unalignedIncluded bool) chan struct{} {
	// build in background
	fin := make(chan struct{})

	if ok := r.buildingFiles.CompareAndSwap(false, true); !ok {
		close(fin)
		return fin
	}

	built := true
	var err error

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer r.buildingFiles.Store(false)
		for built {
			built, err = r.buildFile(r.ctx, num)
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped)) {
				close(fin)
				return
			}
		}

		go func() {
			defer close(fin)
			if err := r.MergeLoop(r.ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
					return
				}
				r.logger.Warn("[forkable snapshots] merge", "err", err)
			}
		}()
	}()

	return fin
}

func (r *ForkableAgg) MergeLoop(ctx context.Context) error {
	// multipe calls not allowed
	// use snaps.MergeConfig...
	//
	return nil
}

// buildFile builds a single file
// multiple invocations will build subsequent files
func (r *ForkableAgg) buildFile(ctx context.Context, to RootNum) (built bool, err error) {
	type wrappedFilesItem struct {
		*filesItem
		st CanonicityStrategy
		id ForkableId
	}
	var (
		g, ctx2     = errgroup.WithContext(ctx)
		cfiles      = make([]*wrappedFilesItem, 0)
		cfilesMu    = sync.Mutex{}
		closeCfiles = true
	)

	defer func() {
		if !closeCfiles {
			return
		}
		for _, df := range cfiles {
			df.closeFiles()
		}
	}()

	g.SetLimit(r.collateAndBuildWorkers)
	defer r.buildingFiles.Store(false)

	// build aligned
	tx := r.BeginTemporalTx()
	defer tx.Close()

	firstRootNumNotInFiles := tx.AlignedMaxRootNum()
	r.loop(func(p *ProtoForkable) error {
		r.wg.Add(1)
		g.Go(func() error {
			defer r.wg.Done()

			fromRootNum := firstRootNumNotInFiles
			if p.unaligned {
				fromRootNum = tx.MaxRootNum(p.a)
			}

			var skip bool
			if err := r.db.View(ctx2, func(dbtx kv.Tx) (err error) {
				if dontskip, err := tx.HasRootNumUpto(ctx2, p.a, to, dbtx); err != nil {
					return err
				} else {
					skip = !dontskip
				}
				return nil
			}); err != nil {
				return err
			}

			if skip {
				r.logger.Debug("skipping", "id", p.a, "from", fromRootNum, "to", to)
				return nil
			}

			df, built, err := p.BuildFile(ctx2, fromRootNum, to, r.db, r.ps)
			if err != nil {
				return err
			}

			if !built {
				return nil
			}
			cfilesMu.Lock()
			cfiles = append(cfiles, &wrappedFilesItem{df, p.strategy, p.a})
			cfilesMu.Unlock()
			return nil
		})

		return nil
	})

	if err := g.Wait(); err != nil {
		return false, err
	}
	closeCfiles = false
	tx.Close() // no need for tx in index building

	for _, df := range cfiles {
		r.loop(func(p *ProtoForkable) error {
			if p.a == df.id {
				p.snaps.IntegrateDirtyFile(df.filesItem)
			}
			return nil
		})
	}

	r.recalcVisibleFiles(r.dirtyFilesEndRootNumMinimax())

	return len(cfiles) > 0, nil
}

func (r *ForkableAgg) Close() {
	if r == nil || r.ctxCancel == nil { // invariant: it's safe to call Close multiple times
		return
	}
	r.ctxCancel()
	r.ctxCancel = nil
	r.wg.Wait()

	r.dirtyFilesLock.Lock()
	defer r.dirtyFilesLock.Unlock()
	r.closeDirtyFiles()
	r.recalcVisibleFiles(r.dirtyFilesEndRootNumMinimax())
}

func (r *ForkableAgg) closeDirtyFiles() {
	wg := &sync.WaitGroup{}
	r.loop(func(p *ProtoForkable) error {
		wg.Add(1)
		go func() {
			// TODO: check if p is not the last value
			// see aggregator#closeDirtyFiles()
			defer wg.Done()
			p.snaps.Close()
		}()
		return nil
	})
	wg.Wait()
}

////

func (r *ForkableAgg) openFolder() error {
	eg := &errgroup.Group{}
	r.loop(func(p *ProtoForkable) error {
		eg.Go(func() error {
			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			default:
			}
			return p.snaps.OpenFolder()
		})
		return nil
	})
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("openFolder: %w", err)
	}
	r.recalcVisibleFiles(r.dirtyFilesEndRootNumMinimax())

	return nil
}

func (r *ForkableAgg) dirtyFilesEndRootNumMinimax() RootNum {
	dfMiniMaxRootNum := RootNum(MaxUint64)
	r.loop(func(p *ProtoForkable) error {
		if p.unaligned {
			return nil
		}

		dfMiniMaxRootNum = min(dfMiniMaxRootNum, p.snaps.DirtyFilesMaxRootNum())
		return nil
	})
	return dfMiniMaxRootNum
}

func (r *ForkableAgg) recalcVisibleFiles(toRootNum RootNum) {
	defer r.recalcVisibleFilesMinimaxRootNum()
	r.visibleFilesLock.Lock()
	defer r.visibleFilesLock.Unlock()

	dfMiniMaxRootNum := toRootNum
	r.loop(func(p *ProtoForkable) error {
		if !p.unaligned {
			dfMiniMaxRootNum = min(dfMiniMaxRootNum, p.snaps.DirtyFilesMaxRootNum())
		}
		return nil
	})
	vfMinimaxRootNum := dfMiniMaxRootNum
	r.loop(func(p *ProtoForkable) error {
		rn := dfMiniMaxRootNum
		if p.unaligned {
			// unaligned forkables recalcvisible with no restriction
			rn = RootNum(MaxUint64)
		}
		maxr := p.snaps.RecalcVisibleFiles(rn)
		if !p.unaligned {
			vfMinimaxRootNum = min(vfMinimaxRootNum, maxr)
		}

		return nil
	})

	// truncate visible files after vfMinimaxRootNum
	r.loop(func(p *ProtoForkable) error {
		if p.unaligned {
			return nil
		}
		p.snaps.CloseVisibleFilesAfterRootNum(vfMinimaxRootNum)
		return nil
	})

}

func (r *ForkableAgg) recalcVisibleFilesMinimaxRootNum() {
	aggTx := r.BeginTemporalTx()
	defer aggTx.Close()
	r.visibleFilesMinimaxRootNum.Store(aggTx.AlignedMaxRootNum().Uint64())
}

func (r *ForkableAgg) loop(fn func(p *ProtoForkable) error) error {
	for _, ap := range r.marked {
		if err := fn(ap.ProtoForkable); err != nil {
			return err
		}
	}

	for _, ap := range r.unmarked {
		if err := fn(ap.ProtoForkable); err != nil {
			return err
		}
	}

	for _, ap := range r.buffered {
		if err := fn(ap.ProtoForkable); err != nil {
			return err
		}
	}

	return nil
}

type ForkableAggTemporalTx struct {
	f        *ForkableAgg
	marked   []MarkedTxI
	unmarked []UnmarkedTxI
	buffered []BufferedTxI

	// TODO _leakId logic

	mp map[ForkableId]uint32
	// TODO map from forkableId -> stragety+index in array; strategy encoded in lowest 2-bits.
}

func NewForkableAggTemporalTx(r *ForkableAgg) *ForkableAggTemporalTx {
	marked := make([]MarkedTxI, 0, len(r.marked))
	unmarked := make([]UnmarkedTxI, 0, len(r.unmarked))
	buffered := make([]BufferedTxI, 0, len(r.buffered))
	mp := make(map[ForkableId]uint32)

	for i, ap := range r.marked {
		marked = append(marked, ap.BeginTemporalTx())
		mp[ap.a] = (uint32(i) << 2) | uint32(Marked)
	}

	for i, ap := range r.unmarked {
		unmarked = append(unmarked, ap.BeginTemporalTx())
		mp[ap.a] = (uint32(i) << 2) | uint32(Unmarked)
	}

	for i, ap := range r.buffered {
		buffered = append(buffered, ap.BeginTemporalTx())
		mp[ap.a] = (uint32(i) << 2) | uint32(Buffered)
	}

	return &ForkableAggTemporalTx{
		f:        r,
		marked:   marked,
		unmarked: unmarked,
		buffered: buffered,
		mp:       mp,
	}
}

func (r *ForkableAggTemporalTx) Marked(id ForkableId) MarkedTxI {
	index, ok := r.mp[id]
	if !ok {
		panic(fmt.Errorf("forkable %s not found", id))
	}

	return r.marked[index>>2]
}

func (r *ForkableAggTemporalTx) Unmarked(id ForkableId) UnmarkedTxI {
	index, ok := r.mp[id]
	if !ok {
		panic(fmt.Errorf("forkable %s not found", id))
	}
	return r.unmarked[index>>2]
}

func (r *ForkableAggTemporalTx) Buffered(id ForkableId) BufferedTxI {
	index, ok := r.mp[id]
	if !ok {
		panic(fmt.Errorf("forkable %s not found", id))
	}
	return r.buffered[index>>2]
}

func (r *ForkableAgg) BeginTemporalTx() *ForkableAggTemporalTx {
	return NewForkableAggTemporalTx(r)
}

func (r *ForkableAggTemporalTx) AlignedMaxRootNum() RootNum {
	// return aligned max root num of "any" aligned forkable,
	// which is ok since all are expected to be at same height
	return loopOverDebugFiles(r, ee.AllForkableId, true, func(db ForkableFilesTxI) RootNum {
		return db.VisibleFilesMaxRootNum()
	})
}

func (r *ForkableAggTemporalTx) MaxRootNum(forId ForkableId) RootNum {
	// return max root num of the a given forkableId
	return loopOverDebugFiles(r, forId, false, func(db ForkableFilesTxI) RootNum {
		return db.VisibleFilesMaxRootNum()
	})
}

func (r *ForkableAggTemporalTx) HasRootNumUpto(ctx context.Context, forId ForkableId, to RootNum, tx kv.Tx) (bool, error) {
	return loopOverDebugDbs(r, forId, func(db ForkableDbCommonTxI) (bool, error) {
		return db.HasRootNumUpto(ctx, to, tx)
	})
}

func loopOverDebugDbs[R any](r *ForkableAggTemporalTx, forId ForkableId, fn func(ForkableDbCommonTxI) (R, error)) (R, error) {
	for i, mt := range r.marked {
		if r.f.marked[i].a == forId {
			dbg := mt.(ForkableDebugAPI[MarkedDbTxI])
			return fn(dbg.DebugDb())
		}
	}

	for i, ut := range r.unmarked {
		if r.f.unmarked[i].a == forId {
			dbg := ut.(ForkableDebugAPI[UnmarkedDbTxI])
			return fn(dbg.DebugDb())
		}
	}

	for i, bt := range r.buffered {
		if r.f.buffered[i].a == forId {
			dbg := bt.(ForkableDebugAPI[BufferedDbTxI])
			return fn(dbg.DebugDb())
		}
	}

	panic(fmt.Sprintf("no forkable with id %s", forId.String()))
}

func loopOverDebugFiles[R any](r *ForkableAggTemporalTx, forId ForkableId, skipUnaligned bool, fn func(ForkableFilesTxI) R) R {
	for i, mt := range r.marked {
		if skipUnaligned && r.f.marked[i].unaligned {
			continue
		}
		if forId.MatchAll() || r.f.marked[i].a == forId {
			dbg := mt.(ForkableDebugAPI[MarkedDbTxI])
			return fn(dbg.DebugFiles())
		}
	}

	for i, ut := range r.unmarked {
		if skipUnaligned && r.f.marked[i].unaligned {
			continue
		}
		if forId.MatchAll() || r.f.unmarked[i].a == forId {
			dbg := ut.(ForkableDebugAPI[UnmarkedDbTxI])
			return fn(dbg.DebugFiles())
		}
	}

	for i, bt := range r.buffered {
		if skipUnaligned && r.f.marked[i].unaligned {
			continue
		}
		if forId.MatchAll() || r.f.buffered[i].a == forId {
			dbg := bt.(ForkableDebugAPI[BufferedDbTxI])
			return fn(dbg.DebugFiles())
		}
	}

	panic(fmt.Sprintf("no forkable with id %s", forId.String()))
}

func (r *ForkableAggTemporalTx) Close() {
	if r == nil || r.f == nil {
		return
	}
	r.f = nil
	for _, mt := range r.marked {
		if mt != nil {
			mt.Close()
		}
	}

	for _, ut := range r.unmarked {
		if ut != nil {
			ut.Close()
		}
	}

	for _, bt := range r.buffered {
		if bt != nil {
			bt.Close()
		}
	}
}
