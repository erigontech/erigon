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
func requireOpens(t *testing.T, m *migrationFixture, msg string) {
	t.Helper()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = m.ChainConfig.ChainName
	snaps := blocksnapshots.NewRoSnapshots(cfg, m.dirs.Snap, log.New())
	defer snaps.Close()
	require.NoError(t, snaps.OpenFolder(), msg)
	require.Equal(t, uint64(crashTailFrom-1), snaps.SegmentsMax(), msg)
}

// dropTail removes the tail blocks' headers from the DB, so only the migration can put them back.
func dropTail(t *testing.T, m *migrationFixture, frozenMax uint64) {
	t.Helper()
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		for n := uint64(crashTailFrom); n < frozenMax; n++ {
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
// with the inputs of an earlier merge left beside the merged segment, and a frontier on the epoch grid,
// where no decimal segment is left once the repacks are done.
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
		name      string
		frozenMax uint64
		dump      func(m *migrationFixture)
	}{
		{"unpruned", crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, crashBounds, logger, all...)
		}},
		{"pruned transactions", crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, crashBounds, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, crashBounds[1:], logger, snaptype2.Transactions)
		}},
		{"merge leftovers", crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, []uint64{0, 3000}, logger, all...)
			dumpDecimalTypes(t, m, crashBounds, logger, all...)
		}},
		{"pruned transactions with merge leftovers", crashChainSize, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, []uint64{0, 3000}, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, crashBounds, logger, snaptype2.Headers, snaptype2.Bodies)
			dumpDecimalTypes(t, m, []uint64{1000, 3000}, logger, snaptype2.Transactions)
			dumpDecimalTypes(t, m, crashBounds[1:], logger, snaptype2.Transactions)
		}},
		{"frontier on the epoch grid", crashTailFrom, func(m *migrationFixture) {
			dumpDecimalTypes(t, m, onGrid, logger, all...)
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
				dropTail(t, m, sc.frozenMax)
				return m
			}

			ref := fresh()
			var steps int
			restore := freezeblocks.SetEpochMigrationCrashHook(func() error { steps++; return nil })
			require.NoError(t, migrate(t, ref, logger))
			restore()
			want := snapState(t, ref.dirs)
			requireOpens(t, ref, "reference run")
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
				requireOpens(t, m, msg)
				for n := uint64(crashTailFrom); n < sc.frozenMax; n++ {
					require.True(t, headerInDB(t, m, n), "%s: tail block %d", msg, n)
				}
				pending, err = freezeblocks.EpochMigrationPending(m.Ctx, m.DB, m.dirs, m.ChainConfig)
				require.NoError(t, err)
				require.False(t, pending, msg)
			}
		})
	}
}
