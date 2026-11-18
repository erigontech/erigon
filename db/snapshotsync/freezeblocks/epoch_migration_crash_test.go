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

package freezeblocks_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/ethconfig"
)

const (
	crashChainSize = 9_500 // past one 8,192 segment, so a pruned start changes which tiers are cut
	crashTailFrom  = 9_216
)

var errCrash = errors.New("crash")

// crashBounds splits [0, crashChainSize) into decimal segments the way a node retires them.
var crashBounds = []uint64{0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, crashChainSize}

// snapState is every file in the snapshots dir, with a hash of the content for segments. Indexes are
// compared by name only: building the transactions index twice from the same segment does not give
// the same bytes.
func snapState(t *testing.T, dirs datadir.Dirs) map[string][32]byte {
	t.Helper()
	des, err := os.ReadDir(dirs.Snap)
	require.NoError(t, err)
	state := make(map[string][32]byte, len(des))
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		if filepath.Ext(de.Name()) != ".seg" {
			state[de.Name()] = [32]byte{}
			continue
		}
		b, err := os.ReadFile(filepath.Join(dirs.Snap, de.Name()))
		require.NoError(t, err)
		state[de.Name()] = sha256.Sum256(b)
	}
	return state
}

// requireOpens checks that every block type is visible up to the tail, which needs each segment's
// indexes to open and the types to share one tiling.
func requireOpens(t *testing.T, m *migrationFixture, tailFrom uint64, msg string) {
	t.Helper()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = m.ChainConfig.ChainName
	snaps := blocksnapshots.NewRoSnapshots(cfg, m.dirs.Snap, log.New())
	defer snaps.Close()
	require.NoError(t, snaps.OpenFolder(), msg)
	require.Equal(t, tailFrom-1, snaps.SegmentsMax(), msg)
}

// dropTail removes the tail blocks' headers from the DB, so only the migration can put them back.
func dropTail(t *testing.T, m *migrationFixture, tailFrom, frozenMax uint64) {
	t.Helper()
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		for n := tailFrom; n < frozenMax; n++ {
			if h := rawdb.ReadHeaderByNumber(tx, n); h != nil {
				if err := tx.Delete(kv.Headers, dbutils.HeaderKey(n, h.Hash())); err != nil {
					return err
				}
			}
		}
		return nil
	}))
}

// TestEpochMigration_CrashAtEveryStep stops the migration after each step that changes the disk or
// the DB, starts it again, and requires the same files, byte for byte, as a run that never stopped,
// with the tail back in the DB and nothing left pending. Each scenario is a datadir shape a node can
// have: everything from block 0, transactions pruned to a start off the epoch grid, either of those
// with the inputs of an earlier merge left beside the merged segment, a frontier on the epoch grid,
// where no decimal segment is left once the repacks are done, transactions with a gap, whose decimal
// segments past the gap are deleted rather than converted, and the last two together.
func TestEpochMigration_CrashAtEveryStep(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	noPrune := dbg.NoPrune()
	dbg.SetNoPrune(true) // a chain this long gets its old blocks pruned, and the decimal segments are dumped from the DB
	src := &migrationFixture{
		ExecModuleTester: createDumpTestKV(t, chain.AllProtocolChanges, crashChainSize),
		dirs:             datadir.New(t.TempDir()),
	}
	dbg.SetNoPrune(noPrune)
	all := snaptype2.BlockSnapshotTypes
	onGrid := append(slices.Clone(crashBounds[:len(crashBounds)-1]), crashTailFrom)
	scenarios := []struct {
		name                string
		tailFrom, frozenMax uint64
		dump                func(m *migrationFixture)
	}{
		{"unpruned", crashTailFrom, crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, crashBounds, logger, all...)
		}},
		{"pruned transactions", crashTailFrom, crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, crashBounds, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, crashBounds[1:], logger, snaptype2.Transactions)
		}},
		{"merge leftovers", crashTailFrom, crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, []uint64{0, 3000}, logger, all...)
			dumpDecimalTypes(t, m, crashBounds, logger, all...)
		}},
		{"pruned transactions with merge leftovers", crashTailFrom, crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, []uint64{0, 3000}, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, crashBounds, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, []uint64{1000, 3000}, logger, snaptype2.Transactions)
			dumpDecimalTypes(t, m, crashBounds[1:], logger, snaptype2.Transactions)
		}},
		{"frontier on the epoch grid", crashTailFrom, crashTailFrom, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, onGrid, logger, all...)
		}},
		{"transactions with a gap", 6144, 7000, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, crashBounds, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, crashBounds[:8], logger, snaptype2.Transactions)
			dumpDecimalTypes(t, m, []uint64{8000, 9000, crashChainSize}, logger, snaptype2.Transactions)
		}},
		{"frontier on the epoch grid with segments above a gap", crashTailFrom, crashTailFrom, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, onGrid, logger, all...)
			dumpDecimalTypes(t, m, []uint64{9300, crashChainSize}, logger, all...)
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			pristine := datadir.New(t.TempDir())
			_, err := snaptype.LoadSalt(pristine.Snap, true, logger)
			require.NoError(t, err)
			sc.dump(&migrationFixture{ExecModuleTester: src.ExecModuleTester, dirs: pristine})

			fresh := func() *migrationFixture {
				m := &migrationFixture{ExecModuleTester: src.ExecModuleTester, dirs: datadir.New(t.TempDir())}
				require.NoError(t, os.CopyFS(m.dirs.Snap, os.DirFS(pristine.Snap)))
				dropTail(t, m, sc.tailFrom, sc.frozenMax)
				return m
			}

			ref := fresh()
			var steps int
			restore := freezeblocks.SetEpochMigrationCrashHook(func() error { steps++; return nil })
			require.NoError(t, migrate(t, ref, logger))
			restore()
			want := snapState(t, ref.dirs)
			requireOpens(t, ref, sc.tailFrom, "reference run")
			require.Greater(t, steps, 10)

			for crashAt := 1; crashAt <= steps; crashAt++ {
				m := fresh()
				var n int
				restore := freezeblocks.SetEpochMigrationCrashHook(func() error {
					if n++; n == crashAt {
						return errCrash
					}
					return nil
				})
				err := migrate(t, m, logger)
				restore()
				require.ErrorIs(t, err, errCrash, "step %d", crashAt)
				pending, err := freezeblocks.EpochMigrationPending(m.Ctx, m.DB, m.dirs, m.ChainConfig)
				require.NoError(t, err)
				require.True(t, pending, "step %d: entry points must still see the migration as unfinished", crashAt)

				require.NoError(t, migrate(t, m, logger), "resume after step %d", crashAt)
				msg := fmt.Sprintf("crash after step %d of %d", crashAt, steps)
				require.Equal(t, want, snapState(t, m.dirs), msg)
				requireOpens(t, m, sc.tailFrom, msg)
				for n := sc.tailFrom; n < sc.frozenMax; n++ {
					require.True(t, headerInDB(t, m, n), "%s: tail block %d", msg, n)
				}
				pending, err = freezeblocks.EpochMigrationPending(m.Ctx, m.DB, m.dirs, m.ChainConfig)
				require.NoError(t, err)
				require.False(t, pending, msg)
			}
		})
	}
}

// A migrated segment above the 1024 tier stands in for the file the merger would have written from
// the 1024 segments, so it has to come out byte-identical: compressed, with the merger's settings.
func TestEpochMigration_WritesMergedTiersLikeTheMerger(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	noPrune := dbg.NoPrune()
	dbg.SetNoPrune(true)
	m := &migrationFixture{
		ExecModuleTester: createDumpTestKV(t, chain.AllProtocolChanges, crashChainSize),
		dirs:             datadir.New(t.TempDir()),
	}
	dbg.SetNoPrune(noPrune)
	_, err := snaptype.LoadSalt(m.dirs.Snap, true, logger)
	require.NoError(t, err)
	dumpDecimalTypes(t, m, crashBounds, logger, snaptype2.BlockSnapshotTypes...)
	require.NoError(t, migrate(t, m, logger))

	for _, typ := range snaptype2.BlockSnapshotTypes {
		migrated := typ.FileInfo(m.dirs.Snap, true, 0, 8_192).Path
		merged := filepath.Join(t.TempDir(), filepath.Base(migrated))
		writeLikeTheMerger(t, migrated, merged, logger)
		want, err := os.ReadFile(merged)
		require.NoError(t, err)
		got, err := os.ReadFile(migrated)
		require.NoError(t, err)
		require.True(t, bytes.Equal(want, got), "%s differs from the merger's output", filepath.Base(migrated))
	}
}

// writeLikeTheMerger copies the words of src into dst the way Merger.merge writes a target file.
func writeLikeTheMerger(t *testing.T, src, dst string, logger log.Logger) {
	t.Helper()
	d, err := seg.NewDecompressor(src)
	require.NoError(t, err)
	defer d.Close()
	cfg := seg.DefaultCfg
	cfg.Workers = 1
	c, err := seg.NewCompressor(t.Context(), "merge", dst, t.TempDir(), cfg, log.LvlTrace, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	g := d.MakeGetter()
	var word []byte
	for g.HasNext() {
		word, _ = g.Next(word[:0])
		require.NoError(t, c.AddWord(word))
	}
	require.NoError(t, c.Compress())
}
