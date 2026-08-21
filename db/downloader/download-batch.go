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
	"golang.org/x/sync/semaphore"
)

// Caps concurrent whole-file hashing by kept-snapshot seeding.
var seedConcurrency = runtime.GOMAXPROCS(-1) * 16

type downloadBatch struct {
	d      *Downloader
	cancel context.CancelCauseFunc
	ctx    context.Context
	// Tasks that must finish before abandoning the batch.
	all      sync.WaitGroup
	torrents []*torrent.Torrent
	// Fetch tasks that should be completed before running afterTasks
	metainfoTasks         sync.WaitGroup
	finishedMetadataTasks atomic.Bool
	// These must be run even if the batch is abandoned.
	afterTasks chan func()
	// Caps concurrent seed hashing.
	seedSem *semaphore.Weighted
	// Cancelled only on genuine abandonment, unlike ctx which also ends on ordinary completion.
	seedCancel context.CancelCauseFunc
	seedCtx    context.Context
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
		// Once queued, seeding must survive the batch: use d.ctx, not batch.ctx or seedCtx.
		me.goSeed(func() { me.d.seedKeptSnapshot(me.d.ctx, item.Name) })
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

func (me *downloadBatch) goSeed(f func()) {
	me.all.Go(func() {
		if me.seedSem.Acquire(me.seedCtx, 1) != nil {
			return
		}
		defer me.seedSem.Release(1)
		f()
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

// A nil cause means the batch finished on its own, which still ends it but lets queued seeding run.
func (me *downloadBatch) abandon(cause error) {
	ended := errors.New("download batch abandoned")
	// seedCtx is a d.ctx child, so it outlives the batch unless it is always released.
	defer me.seedCancel(ended)
	me.cancel(ended)
	if cause != nil {
		me.seedCancel(cause)
	}
	me.all.Wait()
	me.d.decDownloadRequests()
}

func (me *downloadBatch) wait(ctx context.Context) (err error) {
	// An all-kept-local batch has no torrents, so the loop below never samples ctx: a cancelled
	// caller must still surface as an error, or dropped seeding reports success.
	defer func() {
		err = cmp.Or(err, context.Cause(ctx))
		me.abandon(err)
	}()
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
