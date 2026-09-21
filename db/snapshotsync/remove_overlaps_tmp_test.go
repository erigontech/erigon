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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Caplin compresses blob sidecars into the same directory the block snapshots merge in, and its
// in-progress target is a .tmp. Deleting every .tmp here destroys that file mid-write: the dump
// then fails on rename and, because the blob frontier only advances once the whole range is
// dumped, every already-compressed range in that run is redone.
func TestRemoveOverlapsKeepsTmpOfForeignTypes(t *testing.T) {
	dir := t.TempDir()
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, log.New())
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())

	foreign := filepath.Join(dir, "v1.1-014790-014800-blobsidecars.seg.2774720258.tmp")
	own := filepath.Join(dir, "v1.0-000000-000500-headers.seg.123.tmp")
	for _, p := range []string{foreign, own} {
		require.NoError(t, os.WriteFile(p, []byte("in progress"), 0o644))
	}

	require.NoError(t, s.RemoveOverlaps(nil))

	require.FileExists(t, foreign, "a .tmp of a type this collection does not own must be left alone")
	require.NoFileExists(t, own, "this collection's own leftover .tmp must still be cleaned up")
}

// An index's file name segment (e.g. "transactions-to-block") differs from its owning type's
// name (e.g. "transactions"). Ownership must be decided on the resolved type, not that raw
// segment, or a killed index build's .tmp is misattributed to nobody and never cleaned up.
func TestRemoveOverlapsCleansOwnIndexTmp(t *testing.T) {
	dir := t.TempDir()
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, log.New())
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())

	ownIdx := filepath.Join(dir, "v1.0-000000-000500-transactions-to-block.idx.987654321.tmp")
	require.NoError(t, os.WriteFile(ownIdx, []byte("in progress"), 0o644))

	require.NoError(t, s.RemoveOverlaps(nil))

	require.NoFileExists(t, ownIdx, "an index .tmp of a type this collection owns must be cleaned up, even though the index's own name differs from the type's name")
}

// A .tmp whose name does not parse cannot be attributed to any type. Deleting it risks the same
// cross-component damage, so it is left for whoever created it.
func TestRemoveOverlapsKeepsUnparsableTmp(t *testing.T) {
	dir := t.TempDir()
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, log.New())
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())

	junk := filepath.Join(dir, "not-a-snapshot-name.tmp")
	require.NoError(t, os.WriteFile(junk, []byte("x"), 0o644))

	require.NoError(t, s.RemoveOverlaps(nil))
	require.FileExists(t, junk)
}

// The sweep must be reachable without a merge: CaplinSnapshots never calls RemoveOverlaps, so
// without a standalone entry point its own interrupted compressions leak multi-GB .tmp files
// that nothing on a running node ever removes.
func TestRemoveOwnTmpFilesSweepsOnlyOwnTypes(t *testing.T) {
	dir := t.TempDir()
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dir, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, log.New())
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())

	own := filepath.Join(dir, "v1.0-000000-000500-headers.seg.123.tmp")
	foreign := filepath.Join(dir, "v1.1-014790-014800-blobsidecars.seg.2774720258.tmp")
	junk := filepath.Join(dir, "not-a-snapshot-name.tmp")
	for _, p := range []string{own, foreign, junk} {
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	}

	require.NoError(t, s.RemoveOwnTmpFiles())

	require.NoFileExists(t, own)
	require.FileExists(t, foreign, "another collection's in-progress .tmp must survive")
	require.FileExists(t, junk, "an unattributable .tmp must survive")
}
