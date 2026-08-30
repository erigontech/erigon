package downloader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/anacrolix/sync"
	"github.com/anacrolix/torrent"

	"github.com/erigontech/erigon/common/log/v3"
)

// Seeding a kept snapshot hashes the whole file, so the work is CPU-bound with a sequential read:
// GOMAXPROCS sets the scale and the doubling covers read stalls. BuildTorrentFilesIfNeed can afford
// far more because most of its files already have a .torrent and short-circuit; these rarely do.
func defaultSeedConcurrency() int { return max(1, runtime.GOMAXPROCS(-1)*2) }

type downloadBatch struct {
	d      *Downloader
	cancel context.CancelCauseFunc
	// Tasks that must finish before abandoning the batch.
	all      sync.WaitGroup
	torrents []*torrent.Torrent
	// Fetch tasks that should be completed before running afterTasks
	metainfoTasks         sync.WaitGroup
	finishedMetadataTasks atomic.Bool
	// These must be run even if the batch is abandoned.
	afterTasks chan func()
	// Cancelled only when the caller goes away, unlike cancel which also fires on ordinary completion.
	seedCancel  context.CancelCauseFunc
	seedCtx     context.Context
	seedDropped atomic.Int64
	ended       sync.Once
}

// Waits for all the fetches to complete then fires off the thread-safe Torrent methods to configure
// for downloading appropriately.
func (me *downloadBatch) taskWaiter() {
	me.metainfoTasks.Wait()
	close(me.afterTasks)
	me.finishedMetadataTasks.Store(true)
	for t := range me.afterTasks {
		if me.d.ctx.Err() != nil {
			return
		}
		t()
	}
}

func (me *downloadBatch) addDownload(item preverifiedSnapshot) error {
	snapshotTorrent, first, localMetainfo, keptLocal, err := me.d.addPreverifiedSnapshotForDownload(item.InfoHash, item.Name)
	if err != nil {
		return err
	}
	if keptLocal {
		me.goSeed(func() error { return me.d.seedKeptSnapshot(me.seedCtx, item.Name) })
	}
	if !snapshotTorrent.Ok {
		return nil
	}
	t := snapshotTorrent.Value
	me.torrents = append(me.torrents, t)
	if !first {
		return nil
	}
	me.metainfoTasks.Go(func() {
		me.doMetainfoTask(func() func() {
			return me.d.addedFirstDownloader(me.d.ctx, t, localMetainfo, item.Name, item.InfoHash)
		})
	})
	return nil
}

// goSeed runs f under the downloader's seeding cap. The cap is on concurrent hashing, not on
// goroutines: every kept item still gets one, parked in Acquire until a slot frees or seedCtx goes.
func (me *downloadBatch) goSeed(f func() error) {
	me.all.Go(func() {
		if me.d.seedSem.Acquire(me.seedCtx, 1) != nil {
			me.seedDropped.Add(1)
			return
		}
		defer me.d.seedSem.Release(1)
		// Only f's outcome shows abandonment. A pre-call ctx check races the cancel: f can pass it
		// and then bail at its own entry check, seeding nothing and never being counted.
		if err := f(); err != nil && errors.Is(err, context.Cause(me.seedCtx)) {
			me.seedDropped.Add(1)
		}
	})
}

func (me *downloadBatch) addAllItems(ctx context.Context, items []preverifiedSnapshot) error {
	defer func() {
		go me.taskWaiter()
	}()
	for _, it := range items {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		err := me.addDownload(it)
		if err != nil {
			err = fmt.Errorf("downloading snapshot %s (infohash %s): %w", it.Name, it.InfoHash.HexString(), err)
			return err
		}
	}
	return nil
}

func (me *downloadBatch) doMetainfoTask(task func() func()) {
	after := task()
	select {
	case me.afterTasks <- after:
	default:
		panic("should always have capacity")
	}
}

var errBatchEnded = errors.New("download batch ended")

// end joins the batch and returns the cause it ended with. Queued seeding is dropped only when ctx
// goes away, including a cancel arriving during the join, which is why the cause is read after it.
// A batch that failed for its own reasons still seeds what it holds.
func (me *downloadBatch) end(ctx context.Context, cause error) error {
	me.ended.Do(func() {
		ended := cmp.Or(cause, errBatchEnded)
		me.cancel(ended)
		stop := context.AfterFunc(ctx, func() { me.seedCancel(context.Cause(ctx)) })
		defer stop()
		// seedCtx is a d.ctx child, so it outlives the batch unless it is always released.
		defer me.seedCancel(ended)
		me.all.Wait()
		if dropped := me.seedDropped.Load(); dropped > 0 {
			me.d.log(log.LvlWarn, "dropped queued kept-local seeding", "count", dropped)
		}
		me.d.decDownloadRequests()
	})
	return cmp.Or(cause, context.Cause(ctx))
}

func (me *downloadBatch) wait(ctx context.Context) (err error) {
	// An all-kept-local batch has no torrents, so the loop below never samples ctx: a cancelled
	// caller must still surface as an error, or seeding it dropped reports success.
	defer func() { err = me.end(ctx, err) }()
	for _, t := range me.torrents {
		select {
		case <-t.Complete().On():
		case <-t.Closed():
			// Might have been asynchronously deleted. Don't want to get stuck.
			return cmp.Or(context.Cause(ctx), fmt.Errorf("torrent unexpectedly closed: %q", t.Name()))
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}
