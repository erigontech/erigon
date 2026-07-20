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

package snapshotsync

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestRemoveOverlapsDefersUnlinkWhileViewOpen pins the correctness guarantee of the
// bundle-refcount reclamation: a segment file subsumed by a merge must not be unlinked
// from disk while a live View still references it. On the pre-bundle design RemoveOverlaps
// unlinks eagerly (only the fd-close is guarded), so the file vanishes under the reader;
// with the watermark it is retired and deleted only once the pinning View closes.
func TestRemoveOverlapsDefersUnlinkWhileViewOpen(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	// Ten 1k sub-segments per type covering [0,10000) plus a covering [0,10000) that
	// subsumes them, so RemoveOverlaps keeps the covering and retires the subs.
	for from := uint64(0); from < 10_000; from += 1_000 {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}
	for _, snT := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, snT.Enum(), dir, version.V1_0, logger)
	}

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	subPath := filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, 0, 1_000, snaptype2.Headers.Enum()))
	_, err := os.Stat(subPath)
	require.NoError(err, "sub-segment must exist before RemoveOverlaps")

	v := s.View() // pins the generation that still references the sub-segments

	require.NoError(s.RemoveOverlaps(nil))

	_, err = os.Stat(subPath)
	require.NoError(err, "sub-segment must NOT be unlinked while a live View still references it")

	v.Close() // drains the pinning generation -> retired files are reclaimed

	_, err = os.Stat(subPath)
	require.True(os.IsNotExist(err), "sub-segment must be unlinked once the last View drained")
}

// TestRemoveOverlapsProtectsPendingRetired covers the case where a merge has already
// retired the subsumed sub-segments (their files still on disk because a reader pins the
// pre-merge generation) and a later RemoveOverlaps re-discovers those files on disk.
// RemoveOverlaps must recognize them as pending reclamation and NOT unlink them as
// orphans while the reader still mmaps them.
func TestRemoveOverlapsProtectsPendingRetired(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	for from := uint64(0); from < 10_000; from += 1_000 {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	// A reader pins the generation that still shows the sub-segments.
	v := s.View()

	// A merge lands a covering [0,10000) file and retires the subsumed subs. Because v
	// pins the older generation, reclamation is deferred and the sub files stay on disk.
	for _, snT := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, snT.Enum(), dir, version.V1_0, logger)
	}
	require.NoError(s.OpenFolder()) // adopt the covering file into dirty
	s.dirtyLock.Lock()
	var subs []*DirtySegment
	for _, tp := range s.enums {
		s.dirty[tp].Walk(func(segs []*DirtySegment) bool {
			for _, sn := range segs {
				if sn.To()-sn.From() == 1_000 {
					subs = append(subs, sn)
				}
			}
			return true
		})
	}
	for _, sn := range subs {
		s.dirty[sn.Type().Enum()].Delete(sn)
	}
	s.recalcVisibleFiles(s.alignMin, subs) // retire subs to the current generation
	s.dirtyLock.Unlock()

	subPath := filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, 0, 1_000, snaptype2.Headers.Enum()))
	_, err := os.Stat(subPath)
	require.NoError(err, "retired sub must still be on disk while a reader pins it")

	require.NoError(s.RemoveOverlaps(nil))
	_, err = os.Stat(subPath)
	require.NoError(err, "RemoveOverlaps must not unlink a pending-retired sub still pinned by a live reader")

	v.Close()
	_, err = os.Stat(subPath)
	require.True(os.IsNotExist(err), "sub must be reclaimed once the reader drained")
}

// TestReadersRaceRetire stresses the Load()->pin window and the eager-unlink hazard:
// many readers open/close Views (bundle + standalone) on segments that a concurrent
// retire (Delete / RemoveOverlaps) is removing. On the pre-bundle design a reader can
// pin a segment after its last other reader freed it (nil Decompressor -> panic), and
// RemoveOverlaps unlinks files still mmap'd by live readers. Run under -race -count=N.
func TestReadersRaceRetire(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	verOf := func(i int) snaptype.Version {
		if i%2 == 1 {
			return version.V1_1
		}
		return version.V1_0
	}

	// Ten 1k sub-segments per type covering [0, 10000), plus a covering [0,10000)
	// per type so RemoveOverlaps has a keeper and subsumed subs to retire.
	subNames := make([]string, 0, 10*len(snaptype2.BlockSnapshotTypes))
	for from := uint64(0); from < 10_000; from += 1_000 {
		for i, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, verOf(i), logger)
			subNames = append(subNames, snaptype.SegmentFileName(verOf(i), from, from+1_000, snT.Enum()))
		}
	}
	for i, snT := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, snT.Enum(), dir, verOf(i), logger)
	}

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	read := func() {
		v := s.View()
		for _, snT := range snaptype2.BlockSnapshotTypes {
			if seg, ok := v.Segment(snT, 500); ok {
				g := seg.Src().MakeGetter()
				g.Reset(0)
				if g.HasNext() {
					g.Next(nil)
				}
			}
		}
		v.Close()

		if seg, ok, closeFn := s.ViewSingleFile(snaptype2.Headers, 1500); ok {
			g := seg.Src().MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				g.Next(nil)
			}
			closeFn()
		} else {
			closeFn()
		}

		rotx := s.ViewType(snaptype2.Transactions)
		for _, seg := range rotx.Segments {
			g := seg.Src().MakeGetter()
			g.Reset(0)
			if g.HasNext() {
				g.Next(nil)
			}
		}
		rotx.Close()
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					read()
				}
			}
		}()
	}

	// Let readers warm up so they are churning Views when retirement hits.
	time.Sleep(20 * time.Millisecond)

	// Retire subsumed sub-segments concurrently with the readers.
	require.NoError(s.RemoveOverlaps(nil))
	for _, f := range subNames {
		_ = s.retireFiles(f)
	}

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	// With all readers gone, one more View/Close collapses the generation chain to a
	// single node and reclaims every retired file — no leak.
	s.View().Close()
	require.Equal(s.visible.Load(), s.oldestVisible, "generation chain must collapse once readers drain")
	for _, f := range subNames {
		_, err := os.Stat(filepath.Join(dir, f))
		require.True(os.IsNotExist(err), "retired sub-segment %s must be unlinked after drain", f)
	}
}
