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

package blob_storage

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/common"
)

type statCountingFs struct {
	afero.Fs
	stats atomic.Int64
}

func (f *statCountingFs) Stat(name string) (os.FileInfo, error) {
	f.stats.Add(1)
	return f.Fs.Stat(name)
}

func newStatCountingColumnStore(t *testing.T) (*dataColumnStorageImpl, *statCountingFs) {
	t.Helper()
	fs := &statCountingFs{Fs: afero.NewMemMapFs()}
	return NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl), fs
}

func savedColumns(t *testing.T, s *dataColumnStorageImpl, slot uint64, root common.Hash) []uint64 {
	t.Helper()
	saved, err := s.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	return saved
}

func columnExists(t *testing.T, s *dataColumnStorageImpl, slot uint64, root common.Hash, column int64) bool {
	t.Helper()
	exists, err := s.ColumnSidecarExists(t.Context(), slot, root, column)
	require.NoError(t, err)
	return exists
}

func TestSavedColumnIndexScansARootOnceThenAnswersFromMemory(t *testing.T) {
	s, fs := newStatCountingColumnStore(t)
	root := common.Hash{1}
	for _, column := range []int64{0, 2} {
		require.NoError(t, s.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(1000, column)))
	}

	fs.stats.Store(0)
	require.Equal(t, []uint64{0, 2}, savedColumns(t, s, 1000, root))
	require.Equal(t, int64(globalBeaconConfig.NumberOfColumns), fs.stats.Load(), "the first lookup scans the files")

	fs.stats.Store(0)
	require.Equal(t, []uint64{0, 2}, savedColumns(t, s, 1000, root))
	require.True(t, columnExists(t, s, 1000, root, 2))
	require.False(t, columnExists(t, s, 1000, root, 1))
	require.Zero(t, fs.stats.Load(), "a seeded root must not be answered by stat-ing its columns")
}

func TestColumnWriteAfterSeedingUpdatesTheBitmap(t *testing.T) {
	s, fs := newStatCountingColumnStore(t)
	root := common.Hash{1}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(1000, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root))

	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 3, createTestDataColumnSidecar(1000, 3)))

	fs.stats.Store(0)
	require.Equal(t, []uint64{0, 3}, savedColumns(t, s, 1000, root))
	require.True(t, columnExists(t, s, 1000, root, 3))
	require.Zero(t, fs.stats.Load())
}

func TestFailedColumnWriteOfASeededRootLeavesItsBitClear(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	s := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl)
	root := common.Hash{1}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(1000, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root))
	_, file := s.path(1000, root, 1)
	fs.failWritesAfter(file+tmpSuffix, 1, errInducedFailure)

	require.ErrorIs(t, s.WriteColumnSidecars(t.Context(), root, 1, createTestDataColumnSidecar(1000, 1)), errInducedFailure)

	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root), "a column whose write failed has no file")
	require.False(t, columnExists(t, s, 1000, root, 1))
}

// A full cache gives up its oldest bucket for a newer one, and does not cache a root older
// than everything it holds: lookups of old slots are rare and the files still answer them.
func TestColumnBitmapsEvictTheOldestBucketWhenFull(t *testing.T) {
	s, _ := newStatCountingColumnStore(t)
	s.bitmaps = newColumnBitmaps(2)
	oldest, middle, newest, older := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}
	seed := func(root common.Hash, slot uint64) {
		require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(slot, 0)))
		require.Equal(t, []uint64{0}, savedColumns(t, s, slot, root))
	}
	seed(oldest, subdivisionSlot)
	seed(middle, 2*subdivisionSlot)
	seed(newest, 3*subdivisionSlot)
	require.NotContains(t, s.bitmaps.entries, oldest)
	require.Contains(t, s.bitmaps.entries, middle)
	require.Contains(t, s.bitmaps.entries, newest)

	seed(older, 100)
	require.NotContains(t, s.bitmaps.entries, older)
	require.Len(t, s.bitmaps.entries, 2)
	require.True(t, columnExists(t, s, 100, older, 0))
	require.True(t, columnExists(t, s, subdivisionSlot, oldest, 0))
	require.Equal(t, map[uint64]int{2: 1, 3: 1}, s.bitmaps.perBucket)
}

func TestColumnLookupsOfAnUnstoredRootKeepNoEntry(t *testing.T) {
	s, _ := newStatCountingColumnStore(t)
	for i := range byte(16) {
		root := common.Hash{i}
		require.Empty(t, savedColumns(t, s, 1000, root))
		require.False(t, columnExists(t, s, 1000, root, 0))
	}
	require.Empty(t, s.bitmaps.entries, "roots this node never stored must not grow the map")
}

func TestRemoveColumnSidecarsClearsTheBitmap(t *testing.T) {
	s, _ := newStatCountingColumnStore(t)
	root := common.Hash{1}
	for _, column := range []int64{0, 1} {
		require.NoError(t, s.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(1000, column)))
	}
	require.Equal(t, []uint64{0, 1}, savedColumns(t, s, 1000, root))

	require.NoError(t, s.RemoveColumnSidecars(t.Context(), 1000, root, 0))
	require.Equal(t, []uint64{1}, savedColumns(t, s, 1000, root))
	require.False(t, columnExists(t, s, 1000, root, 0))

	require.NoError(t, s.RemoveColumnSidecars(t.Context(), 1000, root, 1))
	require.NotContains(t, s.bitmaps.entries, root, "a root with no columns left keeps no entry")
	require.Empty(t, savedColumns(t, s, 1000, root))
}

func TestFailedColumnRemoveLetsTheFilesAnswerAgain(t *testing.T) {
	fs := newRemoveFailingFs(afero.NewMemMapFs())
	s := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl)
	root := common.Hash{1}
	for _, column := range []int64{0, 1} {
		require.NoError(t, s.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(1000, column)))
	}
	require.Equal(t, []uint64{0, 1}, savedColumns(t, s, 1000, root))
	_, file := s.path(1000, root, 0)
	fs.failOn[file] = errInducedFailure

	require.ErrorIs(t, s.RemoveColumnSidecars(t.Context(), 1000, root, 0, 1), errInducedFailure)

	require.NotContains(t, s.bitmaps.entries, root)
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root), "the file that stayed must still be served")
	require.True(t, columnExists(t, s, 1000, root, 0))
}

// stallingFailingRemoveFs holds a Remove of path until released, then fails it, leaving the
// file in place.
type stallingFailingRemoveFs struct {
	afero.Fs
	path    string
	entered chan struct{}
	release chan struct{}
}

func (f *stallingFailingRemoveFs) Remove(name string) error {
	if name == f.path {
		close(f.entered)
		<-f.release
		return errInducedFailure
	}
	return f.Fs.Remove(name)
}

// ColumnSidecarExists takes no root lock, so it runs while a remove is in flight. A file that
// the remove then fails to delete must not read as missing in the meantime: peers asking for
// it would be refused and the downloader would fetch it again.
func TestColumnSidecarExistsDuringAFailingRemoveSeesTheFile(t *testing.T) {
	fs := &stallingFailingRemoveFs{Fs: afero.NewMemMapFs(), entered: make(chan struct{}), release: make(chan struct{})}
	s := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl)
	root := common.Hash{1}
	for _, column := range []int64{0, 1} {
		require.NoError(t, s.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(1000, column)))
	}
	require.Equal(t, []uint64{0, 1}, savedColumns(t, s, 1000, root))
	_, fs.path = s.path(1000, root, 0)

	removeDone := make(chan error, 1)
	go func() { removeDone <- s.RemoveColumnSidecars(t.Context(), 1000, root, 0) }()
	<-fs.entered

	require.True(t, columnExists(t, s, 1000, root, 0), "the file is still on disk while its remove is in flight")

	close(fs.release)
	require.ErrorIs(t, <-removeDone, errInducedFailure)
	require.True(t, columnExists(t, s, 1000, root, 0))
	require.Equal(t, []uint64{0, 1}, savedColumns(t, s, 1000, root))
}

func TestColumnBitmapsAreNotUsedBelowThePruneFloor(t *testing.T) {
	s, _ := newStatCountingColumnStore(t)
	pruned, kept := common.Hash{1}, common.Hash{2}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), pruned, 0, createTestDataColumnSidecar(100, 0)))
	require.NoError(t, s.WriteColumnSidecars(t.Context(), kept, 0, createTestDataColumnSidecar(subdivisionSlot, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 100, pruned))
	require.Equal(t, []uint64{0}, savedColumns(t, s, subdivisionSlot, kept))

	require.NoError(t, s.PruneBelow(subdivisionSlot))

	require.NotContains(t, s.bitmaps.entries, pruned, "a pruned root must not keep its entry")
	require.Contains(t, s.bitmaps.entries, kept)
	require.False(t, columnExists(t, s, 100, pruned, 0))
	require.Empty(t, savedColumns(t, s, 100, pruned))
	require.NotContains(t, s.bitmaps.entries, pruned, "a slot below the floor must not be seeded again")
}

// A prune that raised the floor but has not dropped the entries yet must not let a stale
// entry answer for a file it is removing.
func TestColumnBitmapsBelowTheFloorAreIgnoredBeforeTheyAreDropped(t *testing.T) {
	s, _ := newStatCountingColumnStore(t)
	root := common.Hash{1}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(100, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 100, root))

	require.NoError(t, s.pruneBelow(subdivisionSlot))
	require.Contains(t, s.bitmaps.entries, root)

	require.False(t, columnExists(t, s, 100, root, 0))
	require.Empty(t, savedColumns(t, s, 100, root))
}

// A lookup may name any slot of the root's bucket: the gossip path checks availability before
// it validates the slot. Its scan must still exclude a write or remove of the root at the
// real slot, or the seeded bitmap misses or invents a file.
func TestColumnScanAtAnotherSlotOfTheBucketExcludesRemovesOfTheRoot(t *testing.T) {
	fs := &blockingColumnFs{Fs: afero.NewMemMapFs()}
	s := NewDataColumnStore(fs, globalBeaconConfig, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl)
	root := common.Hash{1}
	for _, column := range []int64{0, 1} {
		require.NoError(t, s.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(1000, column)))
	}
	_, statPath := s.path(1001, root, 1)
	_, removePath := s.path(1000, root, 0)
	fs.blockStatPath = statPath
	fs.statEntered = make(chan struct{})
	fs.statRelease = make(chan struct{})
	fs.blockRemovePath = removePath
	fs.removeEntered = make(chan struct{})
	fs.removeRelease = make(chan struct{})
	close(fs.removeRelease)

	scanDone := make(chan []uint64, 1)
	go func() {
		saved, err := s.GetSavedColumnIndex(t.Context(), 1001, root)
		assert.NoError(t, err)
		scanDone <- saved
	}()
	<-fs.statEntered
	removeDone := make(chan error, 1)
	go func() { removeDone <- s.RemoveColumnSidecars(t.Context(), 1000, root, 0) }()
	select {
	case <-fs.removeEntered:
		t.Fatal("a remove of the root interleaved with its scan at another slot of the bucket")
	case <-time.After(100 * time.Millisecond):
	}
	close(fs.statRelease)

	require.Equal(t, []uint64{0, 1}, <-scanDone)
	require.NoError(t, <-removeDone)
	require.Equal(t, []uint64{1}, savedColumns(t, s, 1000, root))
	require.False(t, columnExists(t, s, 1000, root, 0))
}

func TestColumnBitmapAnswersOnlyForItsBucket(t *testing.T) {
	s, fs := newStatCountingColumnStore(t)
	root := common.Hash{1}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(1000, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root))

	fs.stats.Store(0)
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1001, root), "another slot of the bucket names the same files")
	require.Zero(t, fs.stats.Load())

	require.Empty(t, savedColumns(t, s, subdivisionSlot, root), "another bucket holds none of the root's files")
	require.False(t, columnExists(t, s, subdivisionSlot, root, 0))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root), "a miss elsewhere must not displace the entry")
	fs.stats.Store(0)
	require.True(t, columnExists(t, s, 1000, root, 0))
	require.Zero(t, fs.stats.Load())
}

// The prune floor can pass a root's bucket while its entry is still in the map; a lookup at
// a slot above the floor, in another bucket, must not take that entry for its own.
func TestColumnBitmapOfAPrunedBucketDoesNotAnswerAboveTheFloor(t *testing.T) {
	s, _ := newStatCountingColumnStore(t)
	root := common.Hash{1}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(1000, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root))

	require.NoError(t, s.pruneBelow(subdivisionSlot))
	require.Contains(t, s.bitmaps.entries, root)

	require.False(t, columnExists(t, s, subdivisionSlot, root, 0))
	require.Empty(t, savedColumns(t, s, subdivisionSlot, root))
}

func TestColumnBitmapOfAnotherWidthIsNotUsed(t *testing.T) {
	cfg := *globalBeaconConfig
	s := NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl)
	root := common.Hash{1}
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, 0, createTestDataColumnSidecar(1000, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 1000, root))

	cfg.NumberOfColumns++
	last := int64(cfg.NumberOfColumns - 1)
	require.NoError(t, s.WriteColumnSidecars(t.Context(), root, last, createTestDataColumnSidecar(1000, last)))

	require.Equal(t, []uint64{0, uint64(last)}, savedColumns(t, s, 1000, root))
	require.True(t, columnExists(t, s, 1000, root, last))
}

func TestColumnBitmapsStayInStepWithTheFilesUnderConcurrency(t *testing.T) {
	s := NewDataColumnStore(afero.NewMemMapFs(), globalBeaconConfig, beaconevents.NewEventEmitter()).(*dataColumnStorageImpl)
	const slot = subdivisionSlot + 1000
	root, pruned := common.Hash{1}, common.Hash{2}
	columns := int64(globalBeaconConfig.NumberOfColumns)
	require.NoError(t, s.WriteColumnSidecars(t.Context(), pruned, 0, createTestDataColumnSidecar(100, 0)))
	require.Equal(t, []uint64{0}, savedColumns(t, s, 100, pruned))
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 50 {
			assert.NoError(t, s.PruneBelow(subdivisionSlot))
		}
	})
	for w := range 4 {
		wg.Go(func() {
			for i := range 200 {
				column := int64(i+w) % columns
				if i%3 == 0 {
					assert.NoError(t, s.RemoveColumnSidecars(t.Context(), slot, root, column))
				} else {
					assert.NoError(t, s.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(slot, column)))
				}
			}
		})
		wg.Go(func() {
			for i := range 200 {
				_, err := s.GetSavedColumnIndex(t.Context(), slot, root)
				assert.NoError(t, err)
				_, err = s.ColumnSidecarExists(t.Context(), slot, root, int64(i)%columns)
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()

	key := s.columnKey(slot, root)
	scanned, err := s.scanSavedColumns(slot, key)
	require.NoError(t, err)
	require.Equal(t, scanned, savedColumns(t, s, slot, root))
	cached, ok := s.bitmaps.columns(key)
	require.Equal(t, len(scanned) > 0, ok, "a root with files must be seeded after a lookup")
	require.Equal(t, scanned, cached, "every set bit must have its file and every file its bit")
	require.NotContains(t, s.bitmaps.entries, pruned)
	require.False(t, columnExists(t, s, 100, pruned, 0))
	counted := 0
	for _, n := range s.bitmaps.perBucket {
		counted += n
	}
	require.Len(t, s.bitmaps.entries, counted, "per-bucket counts must track the entries")
}
