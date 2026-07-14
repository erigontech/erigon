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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func writeCaplinStateFixture(t *testing.T, dir, table string, from, to uint64, logger log.Logger) (segPath, idxPath string) {
	t.Helper()
	segName := strings.ReplaceAll(snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, to), "beaconblocks", table)
	segPath = filepath.Join(dir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord([]byte{1}))
	require.NoError(t, c.Compress())

	idxPath = strings.TrimSuffix(segPath, ".seg") + ".idx"
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  idxPath,
		LeafSize:   8,
		BaseDataID: from,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	require.NoError(t, idx.AddKey([]byte{1}, 0))
	require.NoError(t, idx.Build(t.Context()))
	return segPath, idxPath
}

// hasDiskOverlap mirrors the publishable integrity check (cmd/utils/app/publishable_check.go):
// it walks the real directory, sorts by (from,to), and reports whether any file's range
// starts before the previous file's end.
func hasDiskOverlap(t *testing.T, dir, table string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	type rng struct{ from, to uint64 }
	var rs []rng
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		fi, _, ok := snaptype.ParseFileName(dir, e.Name())
		if !ok || fi.CaplinTypeString != table {
			continue
		}
		rs = append(rs, rng{fi.From, fi.To})
	}
	sort.Slice(rs, func(i, j int) bool {
		return rs[i].from < rs[j].from || (rs[i].from == rs[j].from && rs[i].to < rs[j].to)
	})
	for i := 1; i < len(rs); i++ {
		if rs[i].from < rs[i-1].to {
			return true
		}
	}
	return false
}

func openTestCaplinStateSnapshots(t *testing.T, dirs datadir.Dirs, table string, logger log.Logger) *CaplinStateSnapshots {
	t.Helper()
	types := SnapshotTypes{
		KeyValueGetters: map[string]KeyValueGetter{table: nil},
		Compression:     map[string]bool{},
	}
	s := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, types, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())
	return s
}

// A genesis-rooted superset that fully covers an interior/trailing subset must not
// appear as two separate covered ranges: DirtySegmentLess sorts the superset first,
// so the interior subset is the case the visible-list subset check must still hide.
func TestCaplinStateRecalcHidesInteriorSubset(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	ranges := s.coveredRangesForType(table)
	require.Len(t, ranges, 1, "interior subset must be hidden by the covering superset")
	require.Equal(t, Range{from: 0, to: 150_000}, ranges[0])
}

// RemoveOverlaps must physically delete a subset .seg/.idx that is fully covered by a
// larger indexed segment of the same type, so the on-disk publishable check stops
// reporting an overlap. Reproduces the Gnosis PendingDepositsDump failure.
func TestCaplinStateRemoveOverlaps(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	// Published shape: genesis-rooted superset, an interior orphan subset that must
	// go, and an abutting forward chunk that must stay (it is not a subset).
	supSeg, supIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)
	subSeg, subIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)
	tailSeg, tailIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 150_000, 200_000, logger)

	require.True(t, hasDiskOverlap(t, dirs.SnapCaplin, table), "fixture must reproduce the on-disk overlap")

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.NoError(t, s.RemoveOverlaps())

	require.NoFileExists(t, subSeg)
	require.NoFileExists(t, subIdx)
	require.FileExists(t, supSeg)
	require.FileExists(t, supIdx)
	require.FileExists(t, tailSeg, "abutting non-subset chunk must be kept")
	require.FileExists(t, tailIdx)
	require.False(t, hasDiskOverlap(t, dirs.SnapCaplin, table), "overlap must be gone after RemoveOverlaps")

	ranges := s.coveredRangesForType(table)
	require.Equal(t, []Range{{from: 0, to: 150_000}, {from: 150_000, to: 200_000}}, ranges)
}

// A covered subset whose .idx is missing (e.g. a process killed between the .seg and
// .idx writes) must still be removed, without panicking on the nil index entry.
func TestCaplinStateRemoveOverlapsSubsetMissingIndex(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	supSeg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)
	subSeg, subIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)
	require.NoError(t, dir2.RemoveFile(subIdx)) // subset .seg present, .idx missing

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.NoError(t, s.RemoveOverlaps())

	require.NoFileExists(t, subSeg, "covered subset must be removed even without its index")
	require.FileExists(t, supSeg)
	require.False(t, hasDiskOverlap(t, dirs.SnapCaplin, table))
}

// A subset must be kept when no fully-present indexed superset covers it: deleting it
// would drop data. Here the "superset" lacks its .idx, so it is not usable yet.
func TestCaplinStateRemoveOverlapsKeepsSubsetWithoutIndexedSuperset(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	supSeg, supIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)
	subSeg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)
	require.NoError(t, dir2.RemoveFile(supIdx)) // superset not indexed → not a valid cover

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.NoError(t, s.RemoveOverlaps())

	require.FileExists(t, subSeg, "subset must survive when its superset is not indexed")
	require.FileExists(t, supSeg)
}
