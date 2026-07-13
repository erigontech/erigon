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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// RemoveOverlaps must not unlink a covered segment while a live CaplinStateView still pins
// the generation that referenced it: the file is retired to the outgoing generation and
// unlinked only once that view closes. The no-reader case (unlink by return) is covered by
// TestCaplinStateRemoveOverlaps.
func TestCaplinStateRemoveOverlapsDefersUnlinkWhileViewOpen(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)                         // covering superset
	subSeg, subIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger) // covered subset

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	v := s.View() // pins the current generation that RemoveOverlaps supersedes

	require.NoError(t, s.RemoveOverlaps())

	require.FileExists(t, subSeg, "covered subset must NOT be unlinked while a live view pins the generation")
	require.FileExists(t, subIdx)

	v.Close() // drains the pinning generation -> retired files reclaimed

	require.NoFileExists(t, subSeg, "covered subset must be unlinked once the pinning view closes")
	require.NoFileExists(t, subIdx)
}

// Many readers churning View() + segment reads concurrently with RemoveOverlaps must never
// crash or observe a torn generation: a reader's pin keeps its files alive until it releases (I3), and
// every retired file is unlinked once all readers drain. -race is the real oracle here — the
// CL side otherwise relies entirely on the shared core's EL race tests.
func TestCaplinStateReadersRaceRemoveOverlaps(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump
	typ := mustCaplinStateType(t, table)

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 200_000, logger) // covering superset (kept, covers slot 0)
	var subSegs []string
	for from := uint64(0); from < 200_000; from += 50_000 {
		seg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, from, from+50_000, logger) // covered subsets (retired)
		subSegs = append(subSegs, seg)
	}

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	read := func() {
		v := s.View()
		if seg, ok := v.VisibleSegment(0, typ); ok {
			g := seg.src.MakeGetter() // touches the Decompressor reclaim would close
			g.Reset(0)
			if g.HasNext() {
				g.Next(nil)
			}
		}
		v.Close()
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
	time.Sleep(20 * time.Millisecond) // let readers warm up so they churn Views when retirement hits

	require.NoError(t, s.RemoveOverlaps())

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	s.View().Close() // final drain collapses the generation chain and reclaims every retired file
	require.Equal(t, s._visibleFiles.visible.Load(), s._visibleFiles.oldest, "generation chain must collapse once readers drain")
	for _, f := range subSegs {
		require.NoFileExists(t, f, "retired subset must be unlinked after all readers drain")
	}
}

// Close is shutdown-only: it closes fds after readers drain but must never unlink, or a normal
// shutdown would delete the whole on-disk state snapshot set (I2b).
func TestCaplinStateCloseRemovesNoFiles(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	segPath, idxPath := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)

	snTypes := SnapshotTypes{
		KeyValueGetters: map[CaplinStateType]KeyValueGetter{mustCaplinStateType(t, table): nil},
		Compression:     map[CaplinStateType]bool{},
	}
	s := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, snTypes, logger)
	require.NoError(t, s.OpenFolder())

	s.Close()

	require.FileExists(t, segPath, "Close must never unlink state segment files")
	require.FileExists(t, idxPath, "Close must never unlink state index files")
}

// Close must not close fds of segments an older, still-pinned view references. A view pins g0;
// a re-open publishes g1 over the same still-dirty segment; Close then publishes an empty g2.
// The g0 view can still reach the shared DirtySegment, so its fds must stay open until it
// drains — gating the close on the outgoing generation alone would be a use-after-close.
func TestCaplinStateCloseKeepsFdsForPinnedOlderGeneration(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot
	typ := mustCaplinStateType(t, table)

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	v := s.View() // pins g0
	defer v.Close()
	require.NoError(t, s.OpenFolder()) // publishes g1 over the same still-dirty segment; g0 stays pinned

	s.Close() // publishes empty g2; g0 still pinned, so the chain has not drained

	segs := v.VisibleSegments(typ)
	require.NotEmpty(t, segs, "pinned view must still see its segment")
	require.NotNil(t, segs[0].src.Decompressor, "Close must not close fds a pinned older generation references")

	// Close leaves these fds open while the view pins the generation; release the pin and close
	// them so Windows can unlink the still-mmapped files at t.TempDir cleanup (POSIX unlinks regardless).
	v.Close()
	for _, seg := range segs {
		seg.src.close()
	}
}

// OpenList publishes exactly one generation, even when it both detaches a stale file and opens
// a new one, so no reader ever observes a transient set with the stale file gone but the new
// file not yet visible. Pinning the pre-reopen generation lets the chain length measure the
// number of publishes: one hop (pinned -> current) means a single publish.
func TestCaplinStateOpenListSinglePublishNoTransient(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot
	typ := mustCaplinStateType(t, table)

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)                  // A (kept)
	cSeg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 200_000, 250_000, logger) // C (goes stale)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	v := s.View() // pin so the pre-reopen generation cannot be reclaimed
	defer v.Close()
	pinned := s._visibleFiles.visible.Load()
	require.Equal(t, pinned, s._visibleFiles.oldest, "chain must be collapsed to the pinned generation")

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger) // B (new)

	// Drop C from the open list to make it stale. Excluding it drives the same retire path
	// as an on-disk removal; the pinned generation defers its reclaim until v closes, and
	// reclaim closes the segment before unlinking, so no still-mmapped file is unlinked.
	cName := filepath.Base(cSeg)
	var kept []string
	for _, f := range listAllSegFilesInDir(dirs.SnapCaplin) {
		if f != cName {
			kept = append(kept, f)
		}
	}
	require.NoError(t, s.OpenList(kept, true))

	require.NotEqual(t, pinned, s._visibleFiles.visible.Load(), "OpenList must publish a new generation")
	require.Same(t, s._visibleFiles.visible.Load(), pinned.next,
		"OpenList must publish exactly one generation: the pinned gen links directly to it, no transient in between")
	require.Equal(t, []Range{{from: 0, to: 100_000}, {from: 100_000, to: 150_000}}, s.coveredRangesForType(typ),
		"published generation shows A+B (new B present, stale C gone) — never a transient missing B")
}
