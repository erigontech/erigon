package state

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

type ForkableAgg struct {
	db     kv.RoDB
	dirs   datadir.Dirs
	tmpdir string

	marked          []*Forkable[MarkedTxI]
	unmarked        []*Forkable[UnmarkedTxI]
	alignedEntities []kv.ForkableId

	dirtyFilesLock             sync.Mutex
	visibleFilesLock           sync.RWMutex
	visibleFilesMinimaxRootNum atomic.Uint64

	buildingFiles atomic.Bool
	mergingFiles  atomic.Bool
	mergeDisabled atomic.Bool

	collateAndBuildWorkers int
	mergeWorkers           int
	compressWorkers        int

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
		mergeWorkers:           1,
		compressWorkers:        1,
		ps:                     background.NewProgressSet(),
		// marked:   ap.marked,
		// unmarked: ap.unmarked,
		// buffered: ap.buffered,
	}
}

func (r *ForkableAgg) RegisterMarkedForkable(ap *Forkable[MarkedTxI]) {
	r.marked = append(r.marked, ap)
	if !ap.unaligned {
		r.alignedEntities = append(r.alignedEntities, ap.id)
	}
}

func (r *ForkableAgg) RegisterUnmarkedForkable(ap *Forkable[UnmarkedTxI]) {
	r.unmarked = append(r.unmarked, ap)
	if !ap.unaligned {
		r.alignedEntities = append(r.alignedEntities, ap.id)
	}
}

func (r *ForkableAgg) SetCollateAndBuildWorkers(n int) {
	r.collateAndBuildWorkers = n
}

func (r *ForkableAgg) SetMergeWorkers(n int) {
	r.mergeWorkers = n
}

func (r *ForkableAgg) SetCompressWorkers(n int) {
	r.compressWorkers = n
}

func (r *ForkableAgg) SetMergeDisabled(disabled bool) {
	r.mergeDisabled.Store(disabled)
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

func (r *ForkableAgg) BuildFiles(num RootNum) error {
	finished := r.BuildFilesInBackground(num)
	if !(r.buildingFiles.Load() || r.mergingFiles.Load()) {
		return nil
	}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
Loop:
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-finished:
			break Loop
		case <-logEvery.C:
			if !(r.buildingFiles.Load() || r.mergingFiles.Load()) {
				break Loop
			}
			if r.HasBackgroundFilesBuild() {
				r.logger.Info("[fork_agg] Files build", "progress", r.BackgroundProgress())
			}
		}
	}

	return nil
}

// BuildFilesInBackground builds all snapshots (asynchronously) upto a given RootNum
// num is exclusive
func (r *ForkableAgg) BuildFilesInBackground(num RootNum) chan struct{} {
	// build in background
	fin := make(chan struct{})

	if ok := r.buildingFiles.CompareAndSwap(false, true); !ok {
		r.logger.Debug("[fork_agg] BuildFilesInBackground disabled or already in progress. Skipping...")
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
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped)) {
				r.logger.Warn("[fork_agg] buildFile cancelled/stopped", "err", err)
				close(fin)
				return
			} else if err != nil {
				panic(err)
			}
		}

		go func() {
			defer func() {
				close(fin)
			}()
			if err := r.MergeLoop(r.ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
					r.logger.Debug("[fork_agg] MergeLoop cancelled/stopped", "err", err)
					return
				}
				r.logger.Warn("[fork_agg] merge", "err", err)
			}
		}()
	}()

	return fin
}

func (a *ForkableAgg) WaitForBuildAndMerge(ctx context.Context) chan struct{} {
	res := make(chan struct{})
	go func() {
		defer close(res)

		chkEvery := time.NewTicker(30 * time.Second)
		defer chkEvery.Stop()
		for a.buildingFiles.Load() || a.mergingFiles.Load() {
			select {
			case <-ctx.Done():
				return
			case <-chkEvery.C:
				a.logger.Debug("[fork_agg] waiting for files",
					"building files", a.buildingFiles.Load(),
					"merging files", a.mergingFiles.Load())
			}
		}
	}()
	return res
}

func (r *ForkableAgg) MergeLoop(ctx context.Context) (err error) {
	if dbg.NoMerge() || r.mergeDisabled.Load() || !r.mergingFiles.CompareAndSwap(false, true) {
		r.logger.Debug("[fork_agg] MergeLoop disabled or already in progress. Skipping...")
		return nil
	}
	// Merge is background operation. It must not crush application.
	// Convert panic to error.
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("[fork_agg] background files merge: %s, %s", rec, dbg.Stack())
		}
	}()

	r.wg.Add(1)
	defer r.wg.Done()
	defer r.mergingFiles.Store(false)

	somethingMerged := true
	for somethingMerged {
		somethingMerged, err = r.mergeLoopStep(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ForkableAgg) mergeLoopStep(ctx context.Context) (somethingMerged bool, err error) {
	aggTx := r.BeginTemporalTx()
	defer aggTx.Close()

	mf := NewForkableMergeFiles(len(r.marked), len(r.unmarked))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(r.mergeWorkers)
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}

		if err2 := recover(); err2 != nil {
			err = fmt.Errorf("[fork_agg] merge: %s, %s", err2, dbg.Stack())
		}
	}()

	mergeFn := func(proto *ProtoForkable, vfs VisibleFiles, repo *SnapshotRepo, mergedFileToSet **FilesItem) {
		if len(vfs) == 0 {
			return
		}
		endTxNum := RootNum(vfs.EndRootNum())
		mergeRange := repo.FindMergeRange(endTxNum, vfs)

		if !mergeRange.needMerge {
			return
		}

		var subset visibleFiles
		for _, vf := range vfs {
			if mergeRange.from <= vf.StartRootNum() && vf.EndRootNum() <= mergeRange.to {
				if len(subset) > 0 && subset.EndTxNum() != vf.StartRootNum() {
					panic("expected contiguous files")
				}
				vf1, ok := vf.(visibleFile)
				if !ok {
					panic("expected visibleFile")
				}
				subset = append(subset, vf1)
			}
		}

		if subset.StartTxNum() != mergeRange.from {
			r.logger.Error("[fork_agg] merge", "start_file_not_matched", subset.StartTxNum(), "merge_range", mergeRange)
			panic("start file not matched")
		}
		if subset.EndTxNum() != mergeRange.to {
			r.logger.Error("[fork_agg] merge", "end_file_not_matched", subset.EndTxNum(), "merge_range", mergeRange)
			panic("end file not matched")
		}

		// start merging...
		g.Go(func() (err error) {
			mergedFile, err := proto.MergeFiles(ctx, subset, r.compressWorkers, r.ps)
			if err != nil {
				return err
			}
			*mergedFileToSet = mergedFile
			return
		})
	}

	for i, ap := range aggTx.marked {
		mergeFn(r.marked[i].ProtoForkable, ap.DebugFiles().VisibleFiles(), r.marked[i].Repo(), &mf.marked[i])
	}
	for i, ap := range aggTx.unmarked {
		mergeFn(r.unmarked[i].ProtoForkable, ap.DebugFiles().VisibleFiles(), r.unmarked[i].Repo(), &mf.unmarked[i])
	}

	if err := g.Wait(); err != nil {
		r.logger.Debug("[fork_agg] merge", "err", err)
		return false, err
	}

	closeFiles = false
	r.IntegrateMergeFiles(mf)

	aggTx.Close()
	aggTx = r.BeginTemporalTx()
	defer aggTx.Close()

	for i, mf := range mf.marked {
		r.marked[i].snaps.CleanAfterMerge(mf, aggTx.marked[i].DebugFiles().vfs())
	}

	for i, mf := range mf.unmarked {
		r.unmarked[i].snaps.CleanAfterMerge(mf, aggTx.unmarked[i].DebugFiles().vfs())
	}

	return mf.MergedFilePresent(), nil
}

// buildFile builds a single file
// multiple invocations will build subsequent files
func (r *ForkableAgg) buildFile(ctx context.Context, to RootNum) (built bool, err error) {
	type wrappedFilesItem struct {
		*FilesItem
		id kv.ForkableId
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
				fromRootNum = tx.MaxRootNum(p.id)
			}

			var skip bool
			if err := r.db.View(ctx2, func(dbtx kv.Tx) (err error) {
				if dontskip, err := tx.HasRootNumUpto(ctx2, p.id, to, dbtx); err != nil {
					return err
				} else {
					skip = !dontskip
				}
				return nil
			}); err != nil {
				return err
			}

			if skip {
				r.logger.Debug("[fork_agg] skipping", "id", p.id, "from", fromRootNum, "to", to)
				return nil
			}

			df, built, err := p.BuildFile(ctx2, fromRootNum, to, r.db, r.compressWorkers, r.ps)
			if err != nil {
				return err
			}

			if !built {
				return nil
			}
			cfilesMu.Lock()
			cfiles = append(cfiles, &wrappedFilesItem{df, p.id})
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

	if len(cfiles) == 0 {
		return false, nil
	}

	for _, df := range cfiles {
		r.loop(func(p *ProtoForkable) error {
			if p.id == df.id {
				p.snaps.IntegrateDirtyFile(df.FilesItem)
			}
			return nil
		})
	}

	r.recalcVisibleFiles()

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
	r.recalcVisibleFiles()
}

func (r *ForkableAgg) closeDirtyFiles() {
	wg := &sync.WaitGroup{}
	r.loop(func(p *ProtoForkable) error {
		wg.Add(1)
		pc := p
		go func() {
			// TODO: check if p is not the last value
			// see aggregator#closeDirtyFiles()
			defer wg.Done()
			pc.snaps.Close()
		}()
		return nil
	})
	wg.Wait()
}

func (r *ForkableAgg) Tables() (tables []string) {
	for _, m := range r.marked {
		tables = append(tables, m.canonicalTbl, m.valsTbl)
	}
	for _, u := range r.unmarked {
		tables = append(tables, u.valsTbl)
	}
	return
}

func (r *ForkableAgg) BuildMissedAccessors(ctx context.Context, workers int) error {
	startIndexingTime := time.Now()
	ps := background.NewProgressSet()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	go func() {
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				sendDiagnostics(startIndexingTime, ps.DiagnosticsData(), m.Alloc, m.Sys)
				r.logger.Info("[fork_agg] Indexing", "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			}
		}
	}()

	rotx := r.DebugBeginDirtyFilesRo()
	defer rotx.Close()

	missedFilesItems := rotx.FilesWithMissedAccessors()
	if missedFilesItems.IsEmpty() {
		return nil
	}

	for _, m := range r.marked {
		m.BuildMissedAccessors(ctx, g, ps, missedFilesItems.marked[m.id])
	}

	for _, u := range r.unmarked {
		u.BuildMissedAccessors(ctx, g, ps, missedFilesItems.unmarked[u.id])
	}

	if err := g.Wait(); err != nil {
		return err
	}
	rotx.Close()

	return r.OpenFolder()
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
	r.recalcVisibleFiles()

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

// needs dirtyFiles lock to be taken
func (r *ForkableAgg) recalcVisibleFiles() {
	dfMiniMaxRootNum := r.dirtyFilesEndRootNumMinimax()
	defer r.recalcVisibleFilesMinimaxRootNum()
	r.visibleFilesLock.Lock()
	defer r.visibleFilesLock.Unlock()

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
		maxr := p.RecalcVisibleFiles(rn)
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

	return nil
}

func (r *ForkableAgg) IsForkablePresent(id kv.ForkableId) bool {
	for i := range r.marked {
		if r.marked[i].id == id {
			return true
		}
	}

	for i := range r.unmarked {
		if r.unmarked[i].id == id {
			return true
		}
	}
	return false
}

func (r *ForkableAgg) HasBackgroundFilesBuild() bool { return r.ps.Has() }
func (r *ForkableAgg) BackgroundProgress() string    { return r.ps.String() }

type ForkableAggTemporalTx struct {
	f        *ForkableAgg
	marked   []MarkedTxI
	unmarked []UnmarkedTxI
	// TODO _leakId logic

	mp     map[kv.ForkableId]uint32
	logger log.Logger
	// map from forkableId -> stragety+index in array; strategy encoded in lowest 2-bits.
}

func NewForkableAggTemporalTx(r *ForkableAgg) *ForkableAggTemporalTx {
	marked := make([]MarkedTxI, 0, len(r.marked))
	unmarked := make([]UnmarkedTxI, 0, len(r.unmarked))
	mp := make(map[kv.ForkableId]uint32)

	for i, ap := range r.marked {
		marked = append(marked, ap.BeginTemporalTx())
		mp[ap.id] = (uint32(i) << 2) | uint32(kv.Marked)
	}

	for i, ap := range r.unmarked {
		unmarked = append(unmarked, ap.BeginTemporalTx())
		mp[ap.id] = (uint32(i) << 2) | uint32(kv.Unmarked)
	}

	return &ForkableAggTemporalTx{
		f:        r,
		marked:   marked,
		unmarked: unmarked,
		mp:       mp,
		logger:   r.logger,
	}
}

func (r *ForkableAggTemporalTx) IsForkablePresent(id kv.ForkableId) bool {
	_, ok := r.mp[id]
	return ok
}

func (r *ForkableAggTemporalTx) Marked(id kv.ForkableId) MarkedTxI {
	index, ok := r.mp[id]
	if !ok {
		panic(fmt.Errorf("forkable %s not found", Registry.Name(id)))
	}

	return r.marked[index>>2]
}

func (r *ForkableAggTemporalTx) Unmarked(id kv.ForkableId) UnmarkedTxI {
	index, ok := r.mp[id]
	if !ok {
		panic(fmt.Errorf("forkable %s not found", Registry.Name(id)))
	}
	return r.unmarked[index>>2]
}

func (r *ForkableAgg) BeginTemporalTx() *ForkableAggTemporalTx {
	return NewForkableAggTemporalTx(r)
}

func (r *ForkableAggTemporalTx) AlignedMaxRootNum() RootNum {
	// return aligned max root num of "any" aligned forkable,
	// which is ok since all are expected to be at same height
	return loopOverDebugFiles(r, kv.AllForkableId, true, func(db ForkableFilesTxI) RootNum {
		return db.VisibleFilesMaxRootNum()
	})
}

func (r *ForkableAggTemporalTx) MaxRootNum(forId kv.ForkableId) RootNum {
	// return max root num of the a given forkableId
	return loopOverDebugFiles(r, forId, false, func(db ForkableFilesTxI) RootNum {
		return db.VisibleFilesMaxRootNum()
	})
}

func (r *ForkableAggTemporalTx) HasRootNumUpto(ctx context.Context, forId kv.ForkableId, to RootNum, tx kv.Tx) (bool, error) {
	return loopOverDebugDbs(r, forId, func(db ForkableDbCommonTxI) (bool, error) {
		return db.HasRootNumUpto(ctx, to, tx)
	})
}

func (r *ForkableAggTemporalTx) Unwind(ctx context.Context, to RootNum, tx kv.RwTx) error {
	return loopOverDebugDbsExec(r, kv.AllForkableId, func(db ForkableDbCommonTxI) error {
		_, err := db.Unwind(ctx, to, tx)
		return err
	})
}

func (r *ForkableAggTemporalTx) PruneSmallBatches(ctx context.Context, timeout time.Duration, tx kv.RwTx) (hasMore bool, err error) {
	return r.Prune(ctx, r.AlignedMaxRootNum(), timeout, tx)
}

func (r *ForkableAggTemporalTx) Prune(ctx context.Context, toRootNum RootNum, timeout time.Duration, tx kv.RwTx) (hasMore bool, err error) {
	if dbg.NoPrune() {
		return
	}

	limit := uint64(1000)
	if timeout > 5*time.Hour {
		limit = 100_000_000
	} else if timeout >= time.Minute {
		limit = 100_000
	}

	localTimeout := time.NewTicker(timeout)
	defer localTimeout.Stop()
	timeoutErr := errors.New("prune timeout")

	aggLogEvery := time.NewTicker(600 * time.Second)
	defer aggLogEvery.Stop()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	aggStat := ForkablePruneStat{}

	hasMore = true
	err = loopOverDebugDbsExec(r, kv.AllForkableId, func(db ForkableDbCommonTxI) error {
		stat, err := db.Prune(ctx, toRootNum, limit, aggLogEvery, tx)
		if err != nil {
			return err
		}

		aggStat.Accumulate(&stat)
		if stat.PruneCount == 0 {
			hasMore = false
		}

		select {
		case <-localTimeout.C:
			return timeoutErr
		case <-logEvery.C:
			r.logger.Info("[fork_agg] prune progress", "toRootNum", toRootNum, "stat", aggStat)
		case <-ctx.Done():
			r.logger.Info("[fork_agg] prune cancelled", "toRootNum", toRootNum, "stat", aggStat)
			return ctx.Err()
		default:
			return nil
		}

		return nil
	})
	if errors.Is(err, timeoutErr) {
		r.logger.Warn("[fork_agg] prune timeout")
		return
	}
	if !aggStat.PrunedNothing() {
		r.logger.Info("[fork_agg] prune finished", "toRootNum", toRootNum, "stat", aggStat)
	}
	return
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
}

func (r ForkableAggTemporalTx) Ids() (ids []kv.ForkableId) {
	for _, mt := range r.marked {
		ids = append(ids, mt.Id())
	}
	for _, ut := range r.unmarked {
		ids = append(ids, ut.Id())
	}
	return
}

// loop over all forkables (with some variations)
// assume AllForkableId when needed to exec for all forkables
func loopOverDebugDbsExec(r *ForkableAggTemporalTx, forId kv.ForkableId, fn func(ForkableDbCommonTxI) error) error {
	for i, mt := range r.marked {
		if forId.MatchAll() || r.f.marked[i].id == forId {
			dbg := mt.(ForkableDebugAPI[MarkedDbTxI])
			if err := fn(dbg.DebugDb()); err != nil {
				return err
			}
		}
	}

	for i, ut := range r.unmarked {
		if forId.MatchAll() || r.f.unmarked[i].id == forId {
			dbg := ut.(ForkableDebugAPI[UnmarkedDbTxI])
			if err := fn(dbg.DebugDb()); err != nil {
				return err
			}
		}
	}

	return nil
}

func loopOverDebugDbs[R any](r *ForkableAggTemporalTx, forId kv.ForkableId, fn func(ForkableDbCommonTxI) (R, error)) (R, error) {
	// since only single call can return, doesn't support AllForkableId
	for i, mt := range r.marked {
		if r.f.marked[i].id == forId {
			dbg := mt.(ForkableDebugAPI[MarkedDbTxI])
			return fn(dbg.DebugDb())
		}
	}

	for i, ut := range r.unmarked {
		if r.f.unmarked[i].id == forId {
			dbg := ut.(ForkableDebugAPI[UnmarkedDbTxI])
			return fn(dbg.DebugDb())
		}
	}

	panic("no forkable with id " + Registry.String(forId))
}

func loopOverDebugFiles[R any](r *ForkableAggTemporalTx, forId kv.ForkableId, skipUnaligned bool, fn func(ForkableFilesTxI) R) R {
	for i, mt := range r.marked {
		if skipUnaligned && r.f.marked[i].unaligned {
			continue
		}
		if forId.MatchAll() || r.f.marked[i].id == forId {
			dbg := mt.(ForkableDebugAPI[MarkedDbTxI])
			return fn(dbg.DebugFiles())
		}
	}

	for i, ut := range r.unmarked {
		if skipUnaligned && r.f.unmarked[i].unaligned {
			continue
		}
		if forId.MatchAll() || r.f.unmarked[i].id == forId {
			dbg := ut.(ForkableDebugAPI[UnmarkedDbTxI])
			return fn(dbg.DebugFiles())
		}
	}

	panic("no forkable with id " + Registry.Name(forId))
}

func ForkAggTx(tx kv.Tx, id kv.ForkableId) *ForkableAggTemporalTx {
	if withAggTx, ok := tx.(interface{ AggForkablesTx(kv.ForkableId) any }); ok {
		return withAggTx.AggForkablesTx(id).(*ForkableAggTemporalTx)
	}

	return nil
}
