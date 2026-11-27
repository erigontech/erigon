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

package freezeblocks

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func recoverPanic(f func()) (p any) {
	defer func() { p = recover() }()
	f()
	return nil
}

func TestCaplinSnapshotOpenRejectsUnsupportedVersions(t *testing.T) {
	openers := []struct {
		name string
		open func(*CaplinSnapshots, string) error
	}{
		{name: "folder", open: func(s *CaplinSnapshots, _ string) error { return s.OpenFolder() }},
		{name: "list", open: func(s *CaplinSnapshots, name string) error { return s.OpenList([]string{name}, false) }},
	}
	for _, typ := range []snaptype.Type{snaptype.BeaconBlocks, snaptype.BlobSidecars} {
		for _, ver := range []version.Version{{Major: 0, Minor: 1}, version.V2_0} {
			for _, opener := range openers {
				t.Run(typ.Name()+"/"+ver.String()+"/"+opener.name, func(t *testing.T) {
					dirs := datadir.New(t.TempDir())
					name := typ.FileName(ver, 0, 2)
					writeCaplinSegment(t, dirs, typ, 0, 2, ver, true, func(uint64) []byte { return []byte{1} })
					s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
					t.Cleanup(s.Close)

					panicValue := recoverPanic(func() { _ = opener.open(s, name) })
					require.Contains(t, fmt.Sprint(panicValue), name)
					require.Contains(t, fmt.Sprint(panicValue), "Snapshot file")
				})
			}
		}
	}
}

func TestCaplinSnapshotOpenAcceptsSupportedVersions(t *testing.T) {
	for _, typ := range []snaptype.Type{snaptype.BeaconBlocks, snaptype.BlobSidecars} {
		for _, ver := range []version.Version{version.V1_0, version.V1_1, version.V1_2, {Major: 1, Minor: 9}} {
			t.Run(typ.Name()+"/"+ver.String(), func(t *testing.T) {
				dirs := datadir.New(t.TempDir())
				name := typ.FileName(ver, 0, 2)
				writeCaplinSegment(t, dirs, typ, 0, 2, ver, true, func(uint64) []byte { return []byte{1} })
				s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
				t.Cleanup(s.Close)
				require.NoError(t, s.OpenFolder())
				require.Contains(t, s.Files(), name)
			})
		}
	}
}

func TestELBlockSnapshotOpenKeepsV2Behaviour(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	name := snaptype.SegmentFileName(version.V2_0, 0, 2, snaptype2.Headers.Enum())
	createTestSegmentFile(t, 0, 2, snaptype2.Headers.Enum(), dirs.Snap, version.V2_0, log.New())
	createTestSegmentFile(t, 0, 2, snaptype2.Bodies.Enum(), dirs.Snap, version.V2_0, log.New())
	createTestSegmentFile(t, 0, 2, snaptype2.Transactions.Enum(), dirs.Snap, version.V2_0, log.New())
	s := blocksnapshots.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, log.New())
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())
	require.Contains(t, s.Files(), name)
}

func TestReviewUnsupportedAfterBrokenSegmentStillPanics(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	typ := snaptype.BeaconBlocks
	broken := typ.FileName(version.V1_1, 0, 2)
	writeCaplinSegment(t, dirs, typ, 0, 2, version.V1_1, true, func(uint64) []byte { return []byte{1} })
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, broken), []byte("corrupt"), 0o644))
	unsupported := typ.FileName(version.V2_0, 2, 4)
	writeCaplinSegment(t, dirs, typ, 2, 4, version.V2_0, true, func(uint64) []byte { return []byte{1} })

	s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(s.Close)
	panicValue := recoverPanic(func() { _ = s.OpenFolder() })
	require.Contains(t, fmt.Sprint(panicValue), unsupported)
}

func TestOpenListReportsUnsupportedVersionBeforeIndexError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files func(snaptype.Type) []string
	}{
		{name: "supported-first", files: func(typ snaptype.Type) []string {
			return []string{typ.FileName(typ.Versions().Current, 0, 2), typ.IdxFileName(version.V2_0, 2, 4)}
		}},
		{name: "unsupported-index-first", files: func(typ snaptype.Type) []string {
			return []string{typ.IdxFileName(version.V2_0, 2, 4), typ.FileName(typ.Versions().Current, 0, 2)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			typ := snaptype.BeaconBlocks
			writeCaplinSegment(t, dirs, typ, 0, 2, typ.Versions().Current, true, func(uint64) []byte { return []byte{1} })
			require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, typ.IdxFileName(typ.Versions().Current, 0, 2)), []byte("corrupt"), 0o644))
			unsupported := typ.FileName(version.V2_0, 2, 4)
			writeCaplinSegment(t, dirs, typ, 2, 4, version.V2_0, true, func(uint64) []byte { return []byte{1} })

			s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(s.Close)
			panicValue := recoverPanic(func() { require.NoError(t, s.OpenList(tc.files(typ), false)) })
			require.Contains(t, fmt.Sprint(panicValue), unsupported)
		})
	}
}

func TestOpenListMissingUnsupportedSegmentKeepsBaseResult(t *testing.T) {
	for _, tc := range []struct {
		name       string
		optimistic bool
	}{
		{name: "strict"},
		{name: "optimistic", optimistic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			typ := snaptype.BeaconBlocks
			name := typ.FileName(version.V2_0, 0, 2)
			s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(s.Close)

			var err error
			panicValue := recoverPanic(func() { err = s.OpenList([]string{name}, tc.optimistic) })
			require.Nil(t, panicValue)
			require.NoError(t, err)
			require.Empty(t, s.Files())
		})
	}
}

func TestOpenListReportsUnsupportedIndexAfterBrokenSegment(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt bool
	}{
		{name: "missing"},
		{name: "corrupt", corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			typ := snaptype.BeaconBlocks
			supported := typ.FileName(version.V1_1, 0, 2)
			if tc.corrupt {
				writeCaplinSegment(t, dirs, typ, 0, 2, version.V1_1, true, func(uint64) []byte { return []byte{1} })
				require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, supported), []byte("corrupt"), 0o644))
			}
			unsupported := typ.FileName(version.V2_0, 2, 4)
			writeCaplinSegment(t, dirs, typ, 2, 4, version.V2_0, true, func(uint64) []byte { return []byte{1} })
			unsupportedIndex := typ.IdxFileName(version.V2_0, 2, 4)

			s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(s.Close)
			var err error
			panicValue := recoverPanic(func() { err = s.OpenList([]string{supported, unsupportedIndex}, false) })
			require.Contains(t, fmt.Sprint(panicValue), unsupported)
			require.NoError(t, err)
		})
	}
}

func TestOpenListUsesOpenedBeaconSegmentVersion(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	typ := snaptype.BeaconBlocks
	name := typ.FileName(typ.Versions().Current, 0, 2)
	writeCaplinSegment(t, dirs, typ, 0, 2, typ.Versions().Current, true, func(uint64) []byte { return []byte{1} })
	index := typ.IdxFileName(version.ZeroVersion, 0, 2)
	require.NoError(t, os.Rename(filepath.Join(dirs.Snap, typ.IdxFileName(typ.Versions().Current, 0, 2)), filepath.Join(dirs.Snap, index)))

	s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(s.Close)
	panicValue := recoverPanic(func() { require.NoError(t, s.OpenList([]string{index}, false)) })
	require.Nil(t, panicValue)
	require.Equal(t, []string{name}, s.Files())
}

func TestOpenListKeepsBeaconIndexVersionBehavior(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	typ := snaptype.BeaconBlocks
	name := typ.FileName(version.V1_0, 0, 2)
	writeCaplinSegment(t, dirs, typ, 0, 2, version.V1_0, true, func(uint64) []byte { return []byte{1} })
	index := typ.IdxFileName(version.V2_0, 0, 2)
	require.NoError(t, os.Rename(filepath.Join(dirs.Snap, typ.IdxFileName(version.V1_0, 0, 2)), filepath.Join(dirs.Snap, index)))

	s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(s.Close)
	panicValue := recoverPanic(func() { require.NoError(t, s.OpenList([]string{index, name}, true)) })
	require.Nil(t, panicValue, "listing the same index and segment must keep the folder open result")
	require.Equal(t, []string{name}, s.Files())
}

func TestOpenListRejectsUnsupportedVersionWhenOptimistic(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	typ := snaptype.BeaconBlocks
	name := typ.FileName(version.V2_0, 0, 2)
	writeCaplinSegment(t, dirs, typ, 0, 2, version.V2_0, true, func(uint64) []byte { return []byte{1} })
	s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(s.Close)
	panicValue := recoverPanic(func() { _ = s.OpenList([]string{name}, true) })
	require.Contains(t, fmt.Sprint(panicValue), name)
}
