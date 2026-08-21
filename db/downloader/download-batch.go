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
)

type downloadBatch struct {
	d      *Downloader
	ctx    context.Context
	cancel context.CancelCauseFunc
	// Tasks that must finish before abandoning the batch.
	all      sync.WaitGroup
	torrents []*torrent.Torrent
	// Fetch tasks that should be completed before running afterTasks
	metainfoTasks         sync.WaitGroup
	finishedMetadataTasks atomic.Bool
	// These must be run even if the batch is abandoned.
	afterTasks chan func()
	// Bounds concurrent metainfo derivation, each of which hashes a whole data file.
	seedSlots chan struct{}
}

func maxConcurrentSeedKept() int { return runtime.GOMAXPROCS(-1) * 16 }

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
		me.all.Go(func() { me.seedKeptSnapshot(item.Name) })
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

// seedKeptSnapshot waits for a slot before deriving a metainfo: that hashes the whole data file
// with a piece buffer per hash in flight, and a datadir of rsync'd snapshots needs one per item.
// The wait watches d.ctx rather than the batch, because abandon cancels the batch on every path
// including success, and a kept snapshot that is never registered is never seeded.
func (me *downloadBatch) seedKeptSnapshot(name string) {
	select {
	case me.seedSlots <- struct{}{}:
	case <-me.d.ctx.Done():
		return
	}
	defer func() { <-me.seedSlots }()
	me.d.seedKeptSnapshot(me.d.ctx, name)
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

func (me *downloadBatch) abandon() {
	me.cancel(errors.New("download batch abandoned"))
	me.all.Wait()
	me.d.decDownloadRequests()
}

func (me *downloadBatch) wait(ctx context.Context) error {
	defer me.abandon()
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
