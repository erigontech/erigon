// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package seg

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

const (
	// readPerSecLimit is the max sequential read throughput this controller is designed for.
	readPerSecLimit = 500 * 1024 * 1024 // 500 MiB/s

	// madvPeriod is how often the goroutine polls curPos and issues MADV_WILLNEED / MADV_RANDOM.
	// At readPerSecLimit the reader advances exactly readAheadNotifyStep bytes per tick.
	madvPeriod = 8 * time.Millisecond

	// readAheadNotifyStep = readPerSecLimit × madvPeriod / time.Second.
	// The Getter updates curPos every readAheadNotifyStep bytes consumed, so the
	// controller's view of the read position is at most one step stale.
	readAheadNotifyStep = readPerSecLimit * int64(madvPeriod) / int64(time.Second) // 4 MiB

	// DefaultAheadSize is the recommended MADV_WILLNEED look-ahead window.
	DefaultAheadSize int64 = 4 * 1024 * 1024 // 4 MiB

	// DefaultTrailSize is the recommended trailing MADV_RANDOM window for data >> RAM.
	// Use 0 when data fits comfortably in RAM.
	DefaultTrailSize int64 = 8 * 1024 * 1024 // 8 MiB
)

// ReadAheadController issues MADV_WILLNEED on a sliding window ahead of the
// current read position and MADV_RANDOM on a trailing window of already-consumed
// pages (to reset the OS read-ahead state without evicting pages from the page
// cache).  On Close() the entire prefetched-but-not-yet-released region is reset
// to MADV_RANDOM, so after a full sequential read the whole file is in a clean
// state for future random-access readers.
//
// # Why not MADV_SEQUENTIAL?
//
// MADV_SEQUENTIAL is a per-VMA hint that makes the kernel aggressively read
// ahead and evict pages behind the read head (equivalent to POSIX_FADV_SEQUENTIAL).
// This works well for a single short-lived reader of a file that fits in RAM.
// It is harmful when:
//
//   - Many Getters read different files simultaneously at different offsets
//     (e.g. an RPC server serving concurrent requests).  Aggressive per-reader
//     read-ahead from all of them floods the disk with speculative fetches.
//   - The behind-eviction is global: it drops pages from the shared page cache,
//     hurting every other reader that touches the same file, even from a separate
//     file descriptor or mmap.  Opening a second fd to isolate the VMA still
//     shares the page cache with all other openers of the same inode, so
//     MADV_DONTNEED / behind-eviction on one mapping evicts pages for all of them.
//   - Data >> RAM: pages evicted by one sequential reader are unlikely to be
//     re-read from cache, amplifying both disk I/O and NVMe wear.
//
// # Why MADV_WILLNEED + MADV_RANDOM?
//
// MADV_WILLNEED is an explicit "load these exact pages now" request — not a hint
// to the OS read-ahead engine.  We issue it only for the narrow window we actually
// need, so there is no speculative I/O and no competition between readers.
//
// MADV_RANDOM resets the kernel's read-ahead state for already-consumed pages
// WITHOUT evicting them from the page cache.  Parallel readers that happen to
// access the same region still get cache hits; we merely tell the kernel to stop
// pre-reading beyond what we explicitly request.
//
// After the reader finishes (or calls Close()), the whole file is MADV_RANDOM:
// a subsequent random-access reader (e.g. binary search in a sorted segment) is
// completely unaffected.
//
// Usage:
//
//	g := d.MakeGetter()
//	defer g.StartReadAhead(DefaultAheadSize, 0).Close()
//	for g.HasNext() { ... }
type ReadAheadController struct {
	data      []byte        // sub-slice of the decompressor's mmap (words region)
	curPos    atomic.Uint64 // updated by Getter every readAheadNotifyStep bytes
	stopCh    chan struct{}
	done      chan struct{} // closed when loop() returns
	aheadSize int64         // bytes ahead of curPos to keep warm via MADV_WILLNEED
	trailSize int64         // bytes behind curPos before issuing MADV_RANDOM; 0 = release only on Close
	pageSize  uint64        // cached os.Getpagesize(), for alignment

	// Written by loop(), read by Close() after <-done (safe: channel close is happens-before).
	// Frontiers only advance (monotonically increasing), which is why the design is
	// obviously correct regardless of concurrent curPos changes or backward Reset() calls:
	// whatever curPos does, Close() always flushes [releasedUntil, prefetchedUntil)
	// to MADV_RANDOM, leaving the whole prefetched region in a clean state.
	prefetchedUntil uint64 // leading frontier: MADV_WILLNEED issued up to here
	releasedUntil   uint64 // trailing frontier: MADV_RANDOM issued up to here

	// Replaceable for testing; production code uses the real mmap functions.
	madvWillNeed func([]byte) error
	madvRandom   func([]byte) error
}

func newReadAheadController(data []byte, aheadSize, trailSize int64) *ReadAheadController {
	rac := &ReadAheadController{
		data:         data,
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
		aheadSize:    aheadSize,
		trailSize:    trailSize,
		pageSize:     uint64(os.Getpagesize()),
		madvWillNeed: mmap.MadviseWillNeed,
		madvRandom:   mmap.MadviseRandom,
	}
	go rac.loop()
	return rac
}

// Close stops the background goroutine, waits for it to exit (so no further
// mmap accesses occur), then resets the remaining prefetched-but-not-released
// region to MADV_RANDOM.  After Close the whole file is MADV_RANDOM regardless
// of how much was read.  Safe to call on nil.
func (rac *ReadAheadController) Close() {
	if rac == nil {
		return
	}
	close(rac.stopCh)
	<-rac.done // goroutine has fully exited; prefetchedUntil/releasedUntil are stable
	if rac.releasedUntil < rac.prefetchedUntil {
		if err := rac.madvRandom(rac.data[rac.releasedUntil:rac.prefetchedUntil]); err != nil {
			log.Warn("[read-ahead] final MADV_RANDOM failed", "err", err)
		}
	}
}

// computeRanges is the pure business logic for one poll tick.
// It returns the updated frontier values and the byte slices to pass to
// MADV_WILLNEED (prefetchSlice) and MADV_RANDOM (releaseSlice).
// All boundaries are aligned to rac.pageSize.  Either slice may be nil when no
// work is needed for that side.
func (rac *ReadAheadController) computeRanges(curPos, prefetchedUntil, releasedUntil uint64) (newPrefetchedUntil, newReleasedUntil uint64, prefetchSlice, releaseSlice []byte) {
	dataLen := uint64(len(rac.data))
	ps := rac.pageSize
	newPrefetchedUntil = prefetchedUntil
	newReleasedUntil = releasedUntil

	// Leading window: MADV_WILLNEED [prefetchedUntil, alignUp(curPos+aheadSize))
	rawEnd := min(curPos+uint64(rac.aheadSize), dataLen)
	end := min(pageAlignUp(rawEnd, ps), dataLen)
	if end > prefetchedUntil {
		prefetchSlice = rac.data[prefetchedUntil:end]
		newPrefetchedUntil = end
	}

	// Trailing window: MADV_RANDOM [releasedUntil, alignDown(curPos-trailSize))
	if rac.trailSize > 0 && curPos > uint64(rac.trailSize) {
		releaseEnd := pageAlignDown(curPos-uint64(rac.trailSize), ps)
		if releaseEnd > releasedUntil {
			releaseSlice = rac.data[releasedUntil:releaseEnd]
			newReleasedUntil = releaseEnd
		}
	}
	return
}

func (rac *ReadAheadController) loop() {
	defer close(rac.done)
	ticker := time.NewTicker(madvPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-rac.stopCh:
			return
		case <-ticker.C:
			curPos := rac.curPos.Load()
			pNew, rNew, prefetchSlice, releaseSlice := rac.computeRanges(curPos, rac.prefetchedUntil, rac.releasedUntil)
			rac.prefetchedUntil, rac.releasedUntil = pNew, rNew
			if releaseSlice != nil {
				if err := rac.madvRandom(releaseSlice); err != nil {
					log.Warn("[read-ahead] MADV_RANDOM failed", "err", err)
				}
			}
			if prefetchSlice != nil {
				if err := rac.madvWillNeed(prefetchSlice); err != nil {
					log.Warn("[read-ahead] MADV_WILLNEED failed", "err", err)
				}
			}
		}
	}
}

// pageAlignDown rounds v down to the nearest multiple of pageSize.
// pageSize must be a power of two.
func pageAlignDown(v, pageSize uint64) uint64 { return v &^ (pageSize - 1) }

// pageAlignUp rounds v up to the nearest multiple of pageSize.
// pageSize must be a power of two.
func pageAlignUp(v, pageSize uint64) uint64 { return (v + pageSize - 1) &^ (pageSize - 1) }
