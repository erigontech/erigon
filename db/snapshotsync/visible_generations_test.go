// Copyright 2026 The Erigon Authors
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

package snapshotsync

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
)

// newTestDirtySegment builds a DirtySegment backed by a real .seg file on disk so the core's
// reclaim path (closeAndRemoveFiles) has something observable to unlink.
func newTestDirtySegment(t *testing.T, from, to uint64, dir string, logger log.Logger) (*DirtySegment, string) {
	t.Helper()
	typ := snaptype2.Headers
	createTestSegmentFile(t, from, to, typ.Enum(), dir, version.V1_0, logger)
	sn := NewDirtySegment(typ, version.V1_0, from, to, false)
	require.NoError(t, sn.Open(dir))
	return sn, filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, from, to, typ.Enum()))
}

// A publish with no reader pinning the outgoing generation reclaims and unlinks its retired
// files by the time the call returns (I2 eager reclaim under lock).
func TestVisibleGenerationsPublishDrainedDeletesImmediately(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	seg0, path0 := newTestDirtySegment(t, 0, 1_000, dir, logger)

	var lock sync.RWMutex
	var g visibleGenerations[int]
	g.init(&lock, 0)

	lock.Lock()
	g.publish(1, []*DirtySegment{seg0})
	lock.Unlock()

	require.NoFileExists(path0)
	require.Same(g.current.Load(), g.oldest, "chain collapses when nothing is pinned")
	require.Equal(1, g.currentPayload())
}

// A publish while a reader pins the outgoing generation defers unlink of its retired files
// until that reader releases (I3 drain gate).
func TestVisibleGenerationsPublishDefersWhilePinned(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	seg0, path0 := newTestDirtySegment(t, 0, 1_000, dir, logger)

	var lock sync.RWMutex
	var g visibleGenerations[int]
	g.init(&lock, 0)

	pin := g.acquire() // pins gen0

	lock.Lock()
	g.publish(1, []*DirtySegment{seg0})
	lock.Unlock()

	require.FileExists(path0)
	require.NotSame(g.current.Load(), g.oldest, "chain retains the pinned older generation")

	g.release(pin)

	require.NoFileExists(path0)
	require.Same(g.current.Load(), g.oldest, "chain collapses once the last pin drops")
}

type hazardPayload struct {
	id   int
	file string // backing segment of this generation; retired when the next generation publishes
}

// Under concurrent publish, acquire must never return a generation that was superseded and
// drained between its Load and refcnt increment: such a stale generation's backing file is
// already unlinked, so a reader pinning it would observe a missing file. The hazard-pointer
// re-check in acquire prevents this. Run under -race for the memory-ordering half.
func TestVisibleGenerationsHazardRetry(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	var lock sync.RWMutex
	var g visibleGenerations[hazardPayload]

	const rounds = 200
	segByGen := make([]*DirtySegment, rounds+1)
	seg0, path0 := newTestDirtySegment(t, 0, 1_000, dir, logger)
	segByGen[0] = seg0
	g.init(&lock, hazardPayload{id: 0, file: path0})

	var missing atomic.Bool
	var missingPath atomic.Pointer[string]

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				pin := g.acquire()
				if _, err := os.Stat(pin.payload.file); err != nil {
					fp := pin.payload.file
					missingPath.Store(&fp)
					missing.Store(true)
				}
				g.release(pin)
			}
		}()
	}

	for i := 1; i <= rounds; i++ {
		segI, pathI := newTestDirtySegment(t, uint64(i)*1_000, uint64(i+1)*1_000, dir, logger)
		segByGen[i] = segI
		outgoing := segByGen[i-1]
		lock.Lock()
		g.publish(hazardPayload{id: i, file: pathI}, []*DirtySegment{outgoing})
		lock.Unlock()
	}

	close(stop)
	wg.Wait()

	if missing.Load() {
		require.Failf("pinned generation's backing file was unlinked", "path=%s", *missingPath.Load())
	}
	require.Same(g.current.Load(), g.oldest, "chain must collapse to a single node once readers drain")

	segByGen[rounds].close()
}
