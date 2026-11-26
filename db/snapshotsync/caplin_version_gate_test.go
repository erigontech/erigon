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

package snapshotsync_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestCaplinSnapshotOpenRejectsUnsupportedVersions(t *testing.T) {
	for _, typ := range []snaptype.Type{snaptype.BeaconBlocks, snaptype.BlobSidecars, snaptype.PendingDepositsDump} {
		t.Run(typ.Name(), func(t *testing.T) {
			for _, ver := range []version.Version{{Major: 0, Minor: 1}, version.V2_0} {
				t.Run(ver.String(), func(t *testing.T) {
					open, name, _ := openCaplinFile(t, typ, ver)
					var panicValue any
					func() {
						defer func() { panicValue = recover() }()
						require.NoError(t, open())
					}()
					require.NotNil(t, panicValue, "unsupported Caplin version must panic")
					require.Contains(t, fmt.Sprint(panicValue), name)
					require.Contains(t, fmt.Sprint(panicValue), "Snapshot file")
				})
			}
		})
	}
}

func TestCaplinSnapshotOpenAcceptsSupportedVersions(t *testing.T) {
	for _, typ := range []snaptype.Type{snaptype.BeaconBlocks, snaptype.BlobSidecars, snaptype.PendingDepositsDump} {
		t.Run(typ.Name(), func(t *testing.T) {
			for _, ver := range []version.Version{version.V1_0, version.V1_1, version.V1_2} {
				t.Run(ver.String(), func(t *testing.T) {
					open, name, files := openCaplinFile(t, typ, ver)
					require.NoError(t, open())
					require.Contains(t, files(), name)
				})
			}
		})
	}
}

func TestELBlockSnapshotOpenKeepsV2Behaviour(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	name := writeVersionedSegment(t, dirs.Snap, snaptype2.Headers, version.V2_0)
	writeVersionedSegment(t, dirs.Snap, snaptype2.Bodies, version.V2_0)
	writeVersionedSegment(t, dirs.Snap, snaptype2.Transactions, version.V2_0)
	snapshots := blocksnapshots.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	require.Contains(t, snapshots.Files(), name)
}

func TestCaplinSnapshotOpenListRejectsUnsupportedVersion(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	name := writeVersionedSegment(t, dirs.Snap, snaptype.BeaconBlocks, version.V2_0)
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(snapshots.Close)
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		require.NoError(t, snapshots.OpenList([]string{name}, false))
	}()
	require.NotNil(t, panicValue, "unsupported Caplin version must panic during OpenList")
	require.Contains(t, fmt.Sprint(panicValue), name)
}

func openCaplinFile(t *testing.T, typ snaptype.Type, ver version.Version) (func() error, string, func() []string) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	if typ.Enum() == snaptype.PendingDepositsDump.Enum() {
		name := writeVersionedSegment(t, dirs.SnapCaplin, typ, ver)
		snapshotTypes := snapshotsync.SnapshotTypes{
			KeyValueGetters: map[string]snapshotsync.KeyValueGetter{typ.Name(): nil},
			Compression:     map[string]bool{},
		}
		snapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, snapshotTypes, log.New())
		t.Cleanup(snapshots.Close)
		return snapshots.OpenFolder, name, snapshots.Files
	}

	name := writeVersionedSegment(t, dirs.Snap, typ, ver)
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(snapshots.Close)
	return snapshots.OpenFolder, name, snapshots.Files
}

func writeVersionedSegment(t *testing.T, dir string, typ snaptype.Type, ver version.Version) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	name := typ.FileName(ver, 0, 2)
	info, _, ok := snaptype.ParseFileName(dir, name)
	require.True(t, ok)
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, name), dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord([]byte{1}))
	require.NoError(t, c.Compress())
	require.Equal(t, filepath.Join(dir, name), info.Path)
	for _, index := range typ.Indexes() {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, typ.IdxFileName(ver, 0, 2, index)),
			LeafSize:   8,
		}, log.New())
		require.NoError(t, err)
		idx.DisableFsync()
		require.NoError(t, idx.AddKey([]byte{1}, 0))
		require.NoError(t, idx.Build(t.Context()))
		idx.Close()
	}
	return name
}

func TestOpenListKeepsBeaconIndexVersionBehavior(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	typ := snaptype.BeaconBlocks
	name := writeVersionedSegment(t, dirs.Snap, typ, version.V1_0)
	info, _, ok := snaptype.ParseFileName(dirs.Snap, name)
	require.True(t, ok)
	index := typ.IdxFileName(version.V2_0, info.From, info.To)
	require.NoError(t, os.Rename(filepath.Join(dirs.Snap, typ.IdxFileName(info.Version, info.From, info.To)), filepath.Join(dirs.Snap, index)))

	s := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())
	require.Equal(t, []string{name}, s.Files())

	var got any
	func() {
		defer func() { got = recover() }()
		require.NoError(t, s.OpenList([]string{index, name}, true))
	}()
	require.Nil(t, got, "listing the same index and segment must keep the folder open result")
	require.Equal(t, []string{name}, s.Files())
}
