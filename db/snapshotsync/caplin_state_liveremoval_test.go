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
	"testing"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon/common/dir"
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
	if testing.Short() {
		t.Skip()
	}
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

// OpenList publishes exactly one generation, even when it both detaches a stale file and opens
// a new one, so no reader ever observes a transient set with the stale file gone but the new
// file not yet visible. Pinning the pre-reopen generation lets the chain length measure the
// number of publishes: one hop (pinned -> current) means a single publish.
func TestCaplinStateOpenListSinglePublishNoTransient(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot
	typ := mustCaplinStateType(t, table)

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)                     // A (kept)
	cSeg, cIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 200_000, 250_000, logger) // C (goes stale)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	v := s.View() // pin so the pre-reopen generation cannot be reclaimed
	defer v.Close()
	pinned := s.gens.current.Load()
	require.Equal(t, pinned, s.gens.oldest, "chain must be collapsed to the pinned generation")

	require.NoError(t, dir2.RemoveFile(cSeg))
	require.NoError(t, dir2.RemoveFile(cIdx))
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger) // B (new)

	require.NoError(t, s.OpenFolder())

	require.NotEqual(t, pinned, s.gens.current.Load(), "OpenFolder must publish a new generation")
	require.Same(t, s.gens.current.Load(), pinned.next,
		"OpenFolder must publish exactly one generation: the pinned gen links directly to it, no transient in between")
	require.Equal(t, []Range{{from: 0, to: 100_000}, {from: 100_000, to: 150_000}}, s.coveredRangesForType(typ),
		"published generation shows A+B (new B present, stale C gone) — never a transient missing B")
}
