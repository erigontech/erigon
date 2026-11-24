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

package state

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

func testSegFile(t *testing.T, name string) *seg.Decompressor {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	c, err := seg.NewCompressor(t.Context(), "", path, dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.AddWord([]byte("word")))
	require.NoError(t, c.Compress())
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return d
}

func commitmentVisible(items ...*FilesItem) *aggregatorVisible {
	files := make(visibleFiles, len(items))
	for i, it := range items {
		files[i] = visibleFile{startTxNum: it.startTxNum, endTxNum: it.endTxNum, src: it}
	}
	v := &aggregatorVisible{}
	v.d[kv.CommitmentDomain] = &domainVisible{files: files, name: kv.CommitmentDomain}
	return v
}

type lockCalls struct {
	locked, unlocked []*seg.Decompressor
	fail             error
}

func recordLockCalls(t *testing.T) *lockCalls {
	c := &lockCalls{}
	origLock, origUnlock := lockFile, unlockFile
	lockFile = func(d *seg.Decompressor) error {
		c.locked = append(c.locked, d)
		return c.fail
	}
	unlockFile = func(d *seg.Decompressor) error {
		c.unlocked = append(c.unlocked, d)
		return nil
	}
	t.Cleanup(func() { lockFile, unlockFile = origLock, origUnlock })
	return c
}

func (c *lockCalls) take() (locked, unlocked []*seg.Decompressor) {
	locked, unlocked = c.locked, c.unlocked
	c.locked, c.unlocked = nil, nil
	return locked, unlocked
}

func lockTestItems(t *testing.T, steps ...uint64) []*FilesItem {
	items := make([]*FilesItem, len(steps))
	var from uint64
	for i, n := range steps {
		items[i] = &FilesItem{decompressor: testSegFile(t, fmt.Sprintf("c%d.kv", i)), startTxNum: from, endTxNum: from + n}
		from += n
	}
	return items
}

func decompressors(items ...*FilesItem) []*seg.Decompressor {
	ds := make([]*seg.Decompressor, len(items))
	for i, it := range items {
		ds[i] = it.decompressor
	}
	return ds
}

func lockTestAggregator() *Aggregator {
	a := &Aggregator{logger: log.New()}
	a.stepSize.Store(1)
	return a
}

func TestAggregatorLocksTheTwoCommitmentFilesBeforeTheNewest(t *testing.T) {
	calls := recordLockCalls(t)
	items := lockTestItems(t, 1, 1, 1, 1, 1)
	a := lockTestAggregator()

	a.lockHotCommitmentFiles(commitmentVisible(items[:4]...))
	locked, unlocked := calls.take()
	require.ElementsMatch(t, decompressors(items[1], items[2]), locked)
	require.Empty(t, unlocked)

	a.lockHotCommitmentFiles(commitmentVisible(items...))
	locked, unlocked = calls.take()
	require.Equal(t, decompressors(items[3]), locked, "a new newest file shifts the pair")
	require.Equal(t, decompressors(items[1]), unlocked, "a visible file that left the pair is unlocked")

	a.lockHotCommitmentFiles(commitmentVisible(items[0], items[4]))
	locked, unlocked = calls.take()
	require.Equal(t, decompressors(items[0]), locked)
	require.ElementsMatch(t, decompressors(items[2], items[3]), unlocked, "files that left the visible set are unlocked")

	a.lockHotCommitmentFiles(commitmentVisible(items[4]))
	locked, unlocked = calls.take()
	require.Empty(t, locked)
	require.Equal(t, decompressors(items[0]), unlocked)
	require.Empty(t, a.lockedCommitment)
}

func TestAggregatorDoesNotLockACommitmentFileOverTheStepBound(t *testing.T) {
	calls := recordLockCalls(t)
	items := lockTestItems(t, 256, lockHotMaxSteps+1, lockHotMaxSteps, 1)
	a := lockTestAggregator()

	a.lockHotCommitmentFiles(commitmentVisible(items...))
	locked, unlocked := calls.take()
	require.Equal(t, decompressors(items[2]), locked)
	require.Empty(t, unlocked)
}

func TestAggregatorTriesAFailedCommitmentLockOnce(t *testing.T) {
	calls := recordLockCalls(t)
	calls.fail = errors.New("cannot allocate memory")
	items := lockTestItems(t, 1, 1, 1)
	a := lockTestAggregator()

	a.lockHotCommitmentFiles(commitmentVisible(items...))
	locked, _ := calls.take()
	require.ElementsMatch(t, decompressors(items[0], items[1]), locked)
	require.True(t, a.lockWarned)

	a.lockHotCommitmentFiles(commitmentVisible(items...))
	locked, unlocked := calls.take()
	require.Empty(t, locked, "a failed lock is not tried again for the same file")
	require.Empty(t, unlocked)
}
