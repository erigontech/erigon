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

package antiquary

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

type recordingDownloader struct {
	deleted   [][]string
	seeded    [][]string
	deleteErr error
}

func (d *recordingDownloader) Seed(_ context.Context, paths []string) error {
	d.seeded = append(d.seeded, paths)
	return nil
}

func (d *recordingDownloader) Delete(_ context.Context, paths []string) error {
	d.deleted = append(d.deleted, paths)
	return d.deleteErr
}

func (d *recordingDownloader) Download(context.Context, *downloaderproto.DownloadRequest) error {
	return nil
}

// writeStateFixture writes one indexed caplin state segment, the shape RemoveOverlaps acts on.
func writeStateFixture(t *testing.T, dir, table string, from, to uint64, logger log.Logger) string {
	t.Helper()
	segName := strings.ReplaceAll(snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, to), "beaconblocks", table)
	segPath := filepath.Join(dir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.MinPatternScore = 100
	c, err := seg.NewCompressor(t.Context(), "test", segPath, dir, compressCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord([]byte{1}))
	require.NoError(t, c.Compress())

	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		TmpDir:     dir,
		IndexFile:  strings.TrimSuffix(segPath, ".seg") + ".idx",
		LeafSize:   8,
		BaseDataID: from,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	require.NoError(t, idx.AddKey([]byte{1}, 0))
	require.NoError(t, idx.Build(t.Context()))
	return segPath
}

// overlapAntiquary wires a real CaplinStateSnapshots holding a covered subset, which is what
// makes RemoveOverlaps reach the downloader at all.
func overlapAntiquary(t *testing.T, d *recordingDownloader) (*Antiquary, string) {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	writeStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)
	subSeg := writeStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)

	types := snapshotsync.SnapshotTypes{
		KeyValueGetters: map[string]snapshotsync.KeyValueGetter{table: nil},
		Compression:     map[string]bool{},
	}
	stateSn := snapshotsync.NewCaplinStateSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, types, logger)
	t.Cleanup(stateSn.Close)
	require.NoError(t, stateSn.OpenFolder())

	return &Antiquary{stateSn: stateSn, downloader: d, logger: logger, dirs: dirs}, subSeg
}

// Overlap removal must not be gated on a dump: downloaded files overlap too, and snapgen —
// the only thing that produces a dump — is off by default. A dumpedTo of 0 is the normal node.
func TestRemoveStateOverlapsRunsWithoutDump(t *testing.T) {
	d := &recordingDownloader{}
	a, subSeg := overlapAntiquary(t, d)

	a.removeStateOverlapsAndSeed(context.Background(), 0)

	require.NotEmpty(t, d.deleted, "removal must run when no dump is due")
	require.NoFileExists(t, subSeg, "the covered subset must be unlinked")
	require.Empty(t, d.seeded, "nothing was dumped, so nothing to announce")
}

// A failed Delete makes RemoveOverlaps return before retiring anything, so SegFileNames still
// lists the superseded subsets. Seeding then announces exactly what was handed to Delete.
func TestRemoveStateOverlapsSkipsSeedWhenRemovalFails(t *testing.T) {
	d := &recordingDownloader{deleteErr: errors.New("downloader unreachable")}
	a, subSeg := overlapAntiquary(t, d)

	a.removeStateOverlapsAndSeed(context.Background(), 150_000)

	require.NotEmpty(t, d.deleted)
	require.Empty(t, d.seeded, "a failed removal must not be followed by a seed")
	require.FileExists(t, subSeg, "the failed removal retired nothing")
}

// The ordering the whole change exists for: removal reaches the downloader before the seed.
func TestRemoveStateOverlapsDeletesBeforeSeeding(t *testing.T) {
	d := &recordingDownloader{}
	a, _ := overlapAntiquary(t, d)

	a.removeStateOverlapsAndSeed(context.Background(), 150_000)

	require.NotEmpty(t, d.deleted)
	require.NotEmpty(t, d.seeded, "a completed dump must still be announced")
	for _, seeded := range d.seeded {
		for _, name := range seeded {
			require.NotContains(t, name, "000100000-000150000", "the removed subset must not be announced")
		}
	}
}
