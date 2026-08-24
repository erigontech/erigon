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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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
	mu        sync.Mutex
	calls     []string // "delete" / "seed", in the order they happened
	deleted   [][]string
	seeded    [][]string
	deleteErr error
}

func (d *recordingDownloader) Seed(_ context.Context, paths []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "seed")
	d.seeded = append(d.seeded, paths)
	return nil
}

func (d *recordingDownloader) Delete(_ context.Context, paths []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "delete")
	d.deleted = append(d.deleted, paths)
	return d.deleteErr
}

func (d *recordingDownloader) seededNames() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, batch := range d.seeded {
		for _, p := range batch {
			out = append(out, filepath.Base(p))
		}
	}
	return out
}

func (d *recordingDownloader) deletedNames() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, batch := range d.deleted {
		for _, p := range batch {
			out = append(out, filepath.Base(p))
		}
	}
	return out
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

// overlapStateSnapshots is a real CaplinStateSnapshots holding a covered subset, which is what
// makes RemoveOverlaps reach the downloader at all.
func overlapStateSnapshots(t *testing.T) (*snapshotsync.CaplinStateSnapshots, datadir.Dirs, string) {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	writeStateFixture(t, dirs.SnapCaplin, table, 0, 150_000, logger)
	subSeg := writeStateFixture(t, dirs.SnapCaplin, table, 100_000, 150_000, logger)

	// StateEvents is registered with no files on disk so IndicesMax, and with it
	// BlocksAvailable, stays 0 — that skips the validators-table fill, which would otherwise
	// demand a complete state snapshot set for every slot.
	types := snapshotsync.SnapshotTypes{
		KeyValueGetters: map[string]snapshotsync.KeyValueGetter{table: nil, kv.StateEvents: nil},
		Compression:     map[string]bool{},
	}
	stateSn := snapshotsync.NewCaplinStateSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, types, logger)
	t.Cleanup(stateSn.Close)
	require.NoError(t, stateSn.OpenFolder())
	return stateSn, dirs, subSeg
}

func overlapAntiquary(t *testing.T, d *recordingDownloader) (*Antiquary, string) {
	t.Helper()
	stateSn, dirs, subSeg := overlapStateSnapshots(t)
	return &Antiquary{stateSn: stateSn, downloader: d, logger: log.New(), dirs: dirs}, subSeg
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
	require.FileExists(t, subSeg, "the failed removal retired nothing")
	// The dump still has to be announced — dumpedTo is not carried to the next cadence, so
	// skipping the seed here strands these files until the next full file period.
	require.NotEmpty(t, d.seeded, "a failed removal must not strand the dump")
	for _, name := range d.seededNames() {
		require.NotContains(t, d.deletedNames(), name,
			"a file handed to Delete must not be announced by the same pass")
	}
}

// The ordering the whole change exists for: removal reaches the downloader before the seed.
func TestRemoveStateOverlapsDeletesBeforeSeeding(t *testing.T) {
	d := &recordingDownloader{}
	a, subSeg := overlapAntiquary(t, d)

	a.removeStateOverlapsAndSeed(context.Background(), 150_000)

	require.NotEmpty(t, d.deleted)
	require.NotEmpty(t, d.seeded, "a completed dump must still be announced")
	require.Equal(t, []string{"delete", "seed"}, d.calls,
		"the subset must reach Delete before the dump is hashed and announced")
	require.NotContains(t, d.seededNames(), filepath.Base(subSeg),
		"the removed subset must not be announced")
}

// The wiring only matters where it is called from. Exercising removeStateOverlapsAndSeed
// directly cannot catch the call being deleted from IncrementBeaconState or moved back under
// snapgen, which is off by default and is how a normal node stops removing downloaded overlaps.
func TestIncrementBeaconStateRemovesOverlapsWithSnapgenOff(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, postState, db, t)
	sd := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	sd.OnHeadState(postState)
	vt := state_accessors.NewStaticValidatorTable()

	stateSn, dirs, subSeg := overlapStateSnapshots(t)
	d := &recordingDownloader{}
	ctx := context.Background()

	a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, dirs, d, db,
		stateSn, nil, reader, sd, log.New(), true, true, true, false /* snapgen off */, nil)

	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))

	require.NotEmpty(t, d.deleted, "removal must run from IncrementBeaconState with snapgen off")
	require.NoFileExists(t, subSeg, "the covered subset must be unlinked")
}
