package state

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"golang.org/x/sync/errgroup"
)

type ForkableAgg struct {
	db     kv.RoDB
	dirs   datadir.Dirs
	tmpdir string

	marked   []*Forkable[MarkedTxI]
	unmarked []*Forkable[UnmarkedTxI]
	buffered []*Forkable[BufferedTxI]

	dirtyFilesLock   sync.Mutex
	visibleFilesLock sync.RWMutex

	buildingFiles atomic.Bool
	mergingFiles  atomic.Bool

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

		leakDetector: dbg.NewLeakDetector("forkable_agg", dbg.SlowTx()),
		logger:       logger,
		//ps:          background.NewProgressSet(),

		// marked:   ap.marked,
		// unmarked: ap.unmarked,
		// buffered: ap.buffered,
	}
}

func (r *ForkableAgg) RegisterMarkedForkable(ap *Forkable[MarkedTxI]) {
	r.marked = append(r.marked, ap)
}

func (r *ForkableAgg) RegisterUnmarkedForkable(ap *Forkable[UnmarkedTxI]) {
	r.unmarked = append(r.unmarked, ap)
}

func (r *ForkableAgg) RegisterBufferedForkable(ap *Forkable[BufferedTxI]) {
	r.buffered = append(r.buffered, ap)
}

// - "open folder" 
// - close
// build files
// merge files
// get index
// quick prune
// prune
// debug interface (files/db)
// temporal interface
// begin tx

func (r *ForkableAgg) OpenFolder() error {
	r.dirtyFilesLock.Lock()
	defer r.dirtyFilesLock.Unlock()
	if err := r.openFolder(); err != nil {
		return err
	}

	return nil
}

func (r *ForkableAgg) BuildFiles() {

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
	r.visibleFilesLock.Lock()
	defer r.visibleFilesLock.Unlock()

	dfMiniMaxRootNum := RootNum(toRootNum)
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

	r.loop(func(p *ProtoForkable) error {
		if p.unaligned {
			return nil
		}
		p.snaps.CloseVisibleFilesAfterRootNum(vfMinimaxRootNum)
		return nil
	})

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
