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

type Root struct {
	db     kv.RoDB
	dirs   datadir.Dirs
	tmpdir string

	marked   []*Appendable[MarkedTxI]
	unmarked []*Appendable[UnmarkedTxI]
	buffered []*Appendable[BufferedTxI]

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

func NewAppendableRoot(ctx context.Context, dirs datadir.Dirs, db kv.RoDB, logger log.Logger) *Root {
	ctx, ctxCancel := context.WithCancel(ctx)
	return &Root{
		db:        db,
		dirs:      dirs,
		tmpdir:    dirs.Tmp,
		ctx:       ctx,
		ctxCancel: ctxCancel,

		leakDetector: dbg.NewLeakDetector("appendable_root", dbg.SlowTx()),
		logger:       logger,
		//ps:          background.NewProgressSet(),

		// marked:   ap.marked,
		// unmarked: ap.unmarked,
		// buffered: ap.buffered,
	}
}

func (r *Root) RegisterMarkedAppendable(ap *Appendable[MarkedTxI]) {
	r.marked = append(r.marked, ap)
}

func (r *Root) RegisterUnmarkedAppendable(ap *Appendable[UnmarkedTxI]) {
	r.unmarked = append(r.unmarked, ap)
}

func (r *Root) RegisterBufferedAppendable(ap *Appendable[BufferedTxI]) {
	r.buffered = append(r.buffered, ap)
}

// "open folder"
// close
// build files
// merge files
// get index
// quick prune
// prune
// debug interface (files/db)
// temporal interface
// begin tx

func (r *Root) OpenFolder() error {
	if err := r.openFolder(); err != nil {
		return err
	}

	return nil

}

func (r *Root) openFolder() error {
	r.dirtyFilesLock.Lock()
	defer r.dirtyFilesLock.Unlock()
	eg := &errgroup.Group{}
	r.loopOverAppendables(func(p *ProtoAppendable) error {
		eg.Go(func() error {
			select {
			case <-r.ctx.Done():
				return r.ctx.Err()
			default:
			}
			return p.OpenFolder()
		})
		return nil
	})
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("openFolder: %w", err)
	}

	return nil
}

func (r *Root) loopOverAppendables(fn func(p *ProtoAppendable) error) error {
	for _, ap := range r.marked {
		if err := fn(ap.ProtoAppendable); err != nil {
			return err
		}
	}

	for _, ap := range r.unmarked {
		if err := fn(ap.ProtoAppendable); err != nil {
			return err
		}
	}

	for _, ap := range r.buffered {
		if err := fn(ap.ProtoAppendable); err != nil {
			return err
		}
	}

	return nil
}
