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

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
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
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
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

// TestStackedGenerationsReclaimInOrder pins invariants I3 (drain gate) and the
// oldest->current reclaim order: three generations are stacked, each with its own retired
// file and each pinned by a live View. A drained middle generation must NOT release its
// files while an older generation is still pinned; only when the chain drains from the
// oldest end do the retired files unlink, oldest-first.
func TestStackedGenerationsReclaimInOrder(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	// Ten 1k subs [0,10000) per type plus a covering [0,10000): the subs are hidden by the
	// covering, so retiring a sub is a pure dirty-removal that leaves visible height fixed.
	for from := uint64(0); from < 10_000; from += 1_000 {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, from, from+1_000, snT.Enum(), dir, version.V1_0, logger)
		}
	}
	for _, snT := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, 10_000, snT.Enum(), dir, version.V1_0, logger)
	}

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	// Collapse any open-time generation chain to a single node.
	s.View().Close()
	require.Equal(s.visible.Load(), s.oldestVisible, "chain must be collapsed before stacking")

	sub0 := snaptype.SegmentFileName(version.V1_0, 0, 1_000, snaptype2.Headers.Enum())
	sub1 := snaptype.SegmentFileName(version.V1_0, 1_000, 2_000, snaptype2.Headers.Enum())
	sub0Path := filepath.Join(dir, sub0)
	sub1Path := filepath.Join(dir, sub1)

	g0 := s.visible.Load()
	v0 := s.View() // pins g0

	require.NoError(s.Delete(sub0)) // retires sub0 to g0, publishes g1
	g1 := s.visible.Load()
	require.NotSame(g0, g1)
	v1 := s.View() // pins g1

	require.NoError(s.Delete(sub1)) // retires sub1 to g1, publishes g2
	g2 := s.visible.Load()
	require.NotSame(g1, g2)
	v2 := s.View() // pins g2 (current)

	// All three generations pinned: no retired file may be unlinked yet.
	_, err := os.Stat(sub0Path)
	require.NoError(err, "sub0 must stay on disk while g0 is pinned")
	_, err = os.Stat(sub1Path)
	require.NoError(err, "sub1 must stay on disk while g1 is pinned")

	// Drain the middle generation first. Reclaim walks oldest->current and stops at the
	// still-pinned g0, so NOTHING may be unlinked yet — not even g1's own retired file.
	v1.Close()
	_, err = os.Stat(sub0Path)
	require.NoError(err, "sub0 must stay: g0 still pinned")
	_, err = os.Stat(sub1Path)
	require.NoError(err, "sub1 must stay: reclaim is blocked at the still-pinned older g0")

	// Drain the oldest. Now the chain drains g0 then g1 (both refcnt 0) up to current g2,
	// unlinking both retired files.
	v0.Close()
	_, err = os.Stat(sub0Path)
	require.True(os.IsNotExist(err), "sub0 must be unlinked once g0 drained")
	_, err = os.Stat(sub1Path)
	require.True(os.IsNotExist(err), "sub1 must be unlinked once g0->g1 drained")

	v2.Close()
}

// TestReadPinnedSegmentSurvivesConcurrentRetire stresses the pin+retire hazard: readers
// pin a generation and read the exact segment a concurrent retire is removing. The
// hazard-pointer re-check in acquireVisible and the drain gate together guarantee a
// pinned segment is never closed/unlinked under a live reader. Run under -race.
func TestReadPinnedSegmentSurvivesConcurrentRetire(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	dir := t.TempDir()
	require := require.New(t)

	const n = 20
	names := make(map[snaptype.Enum][]string)
	for i := uint64(0); i < n; i++ {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			createTestSegmentFile(t, i*1_000, (i+1)*1_000, snT.Enum(), dir, version.V1_0, logger)
			names[snT.Enum()] = append(names[snT.Enum()], snaptype.SegmentFileName(version.V1_0, i*1_000, (i+1)*1_000, snT.Enum()))
		}
	}

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()
	require.NoError(s.OpenFolder())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v := s.View()
				segs := v.Segments(snaptype2.Headers)
				if len(segs) > 0 {
					top := segs[len(segs)-1] // the segment most likely being retired next
					g := top.Src().MakeGetter()
					g.Reset(0)
					if g.HasNext() {
						g.Next(nil)
					}
				}
				v.Close()
			}
		}()
	}

	// Let readers warm up so they are pinning generations when retirement hits.
	time.Sleep(20 * time.Millisecond)

	// Retire the top segment (all types together, keeping alignMin heights consistent)
	// from the top down — the exact segment concurrent readers are reading.
	for i := n - 1; i >= 1; i-- {
		del := make([]string, 0, len(snaptype2.BlockSnapshotTypes))
		for _, snT := range snaptype2.BlockSnapshotTypes {
			del = append(del, names[snT.Enum()][i])
		}
		require.NoError(s.Delete(del...))
	}

	close(stop)
	wg.Wait()

	// Drain any lingering pins and confirm the retired files are gone with no leak.
	s.View().Close()
	require.Equal(s.visible.Load(), s.oldestVisible, "generation chain must collapse once readers drain")
	for i := 1; i < n; i++ {
		for _, snT := range snaptype2.BlockSnapshotTypes {
			_, err := os.Stat(filepath.Join(dir, names[snT.Enum()][i]))
			require.True(os.IsNotExist(err), "retired segment %s must be unlinked after drain", names[snT.Enum()][i])
		}
	}
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

	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
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
	for r := 0; r < 8; r++ {
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
		_ = s.Delete(f)
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
