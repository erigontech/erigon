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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const (
	migrationChainSize = 3000 // split into decimal segments [0,1000),[1000,2000),[2000,3000)
	migrationTailFrom  = 2048 // floor(3000, 1024); epoch tiles [0,1024),[1024,2048), tail [2048,3000)
	migrationTailBlock = 2500 // a block inside the tail [2048,3000), used to prove tail writes
)

// decimalBounds are the 1000-aligned block boundaries the fixture splits the decimal data at, so the
// migration reads across several segments and its per-segment deletion advances over more than one.
// The segment holding migrationTailFrom ([2000,3000)) is the straddle; the two below it get deleted
// during the produce phase, and [2000,3000) survives to the final cleanup.
var decimalBounds = []uint64{0, 1000, 2000, migrationChainSize}

// dumpDecimalSeg dumps the real block data for [from,to) into a v1 (decimal-named) segment of typ,
// giving the migration authentic decimal input to convert. firstTxNum seeds the BaseTxnID for a
// bodies segment (ignored by the other types); it returns the last txn id used so a caller can chain
// firstTxNum across consecutive bodies segments.
func dumpDecimalSeg(t *testing.T, m *execmoduletester.ExecModuleTester, from, to uint64, typ snaptype.Type, firstTxNum uint64, logger log.Logger) (lastTxNum uint64) {
	t.Helper()
	cfg := freezeblocks.BlockCompressCfg
	cfg.Workers = 1
	path := filepath.Join(m.Dirs.Snap, snaptype.SegmentFileName(version.V1_0, false, from, to, typ.Enum()))
	c, err := seg.NewCompressor(m.Ctx, "test-decimal", path, m.Dirs.Tmp, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	collect := func(v []byte) error { return c.AddWord(v) }
	switch typ.Enum() {
	case snaptype2.Headers.Enum():
		_, err = freezeblocks.DumpHeadersRaw(m.Ctx, m.DB, m.ChainConfig, from, to, nil, collect, 1, log.LvlDebug, logger, true)
	case snaptype2.Bodies.Enum():
		lastTxNum, err = freezeblocks.DumpBodies(m.Ctx, m.DB, m.ChainConfig, from, to, func(context.Context) uint64 { return firstTxNum }, collect, 1, log.LvlDebug, logger)
	case snaptype2.Transactions.Enum():
		_, err = freezeblocks.DumpTxs(m.Ctx, m.DB, m.ChainConfig, from, to, nil, collect, 1, log.LvlDebug, logger)
	}
	require.NoError(t, err)
	require.NoError(t, c.Compress())
	return lastTxNum
}

// dumpAllDecimal writes the decimal segments for [0,migrationChainSize), split at decimalBounds, for
// all three block types. Bodies chain firstTxNum across segments so BaseTxnID stays cumulative.
func dumpAllDecimal(t *testing.T, m *execmoduletester.ExecModuleTester, logger log.Logger) {
	t.Helper()
	for i := 0; i+1 < len(decimalBounds); i++ {
		dumpDecimalSeg(t, m, decimalBounds[i], decimalBounds[i+1], snaptype2.Headers, 0, logger)
		dumpDecimalSeg(t, m, decimalBounds[i], decimalBounds[i+1], snaptype2.Transactions, 0, logger)
	}
	// DumpBodies returns the next block's BaseTxnID (it advances past the trailing system tx), so the
	// next segment continues from exactly that value — no extra +1.
	var firstTxNum uint64
	for i := 0; i+1 < len(decimalBounds); i++ {
		firstTxNum = dumpDecimalSeg(t, m, decimalBounds[i], decimalBounds[i+1], snaptype2.Bodies, firstTxNum, logger)
	}
}

// migrate runs MigrateDecimalToEpoch against the tester's datadir and DB. It closes the tester's
// snapshots first: in production the migration runs before anything opens a segment, and it deletes
// each decimal segment once its blocks are durable elsewhere. Holding those files open makes that
// delete fail on Windows, where an open mapping blocks removal.
func migrate(t *testing.T, m *execmoduletester.ExecModuleTester, logger log.Logger) error {
	t.Helper()
	m.BlockSnapshots.Close()
	return freezeblocks.MigrateDecimalToEpoch(m.Ctx, m.Dirs, m.DB, m.ChainConfig, 1, logger)
}

// headerInDB reports whether block n's header is present in the DB.
func headerInDB(t *testing.T, m *execmoduletester.ExecModuleTester, n uint64) bool {
	t.Helper()
	var present bool
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		present = rawdb.ReadHeaderByNumber(tx, n) != nil
		return nil
	}))
	return present
}

// dropHeader deletes block n's header from the DB, simulating a tail block whose write-back has not
// been committed yet.
func dropHeader(t *testing.T, m *execmoduletester.ExecModuleTester, n uint64) {
	t.Helper()
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		h := rawdb.ReadHeaderByNumber(tx, n)
		require.NotNil(t, h)
		return tx.Delete(kv.Headers, dbutils.HeaderKey(n, h.Hash()))
	}))
}

// requireEpochOnly asserts a single epoch-only regime: the first epoch tier [0,1024) is present for
// every block type, and none of the decimal segments remain on disk.
func requireEpochOnly(t *testing.T, m *execmoduletester.ExecModuleTester) {
	t.Helper()
	for _, typ := range snaptype2.BlockSnapshotTypes {
		require.FileExists(t, filepath.Join(m.Dirs.Snap, typ.FileInfo(m.Dirs.Snap, true, 0, 1024).Name()), typ.Name())
		for i := 0; i+1 < len(decimalBounds); i++ {
			require.NoFileExists(t, filepath.Join(m.Dirs.Snap, snaptype.SegmentFileName(version.V1_0, false, decimalBounds[i], decimalBounds[i+1], typ.Enum())), typ.Name())
		}
	}
}

// setupMigrated builds a chain, dumps it as decimal, and runs one full migration, leaving the datadir
// in the completed state: epoch [0,1024),[1024,2048) + indexes, decimal deleted, tail [2048,3000) in DB.
func setupMigrated(t *testing.T, logger log.Logger) *execmoduletester.ExecModuleTester {
	t.Helper()
	m := createDumpTestKV(t, chain.AllProtocolChanges, migrationChainSize)
	dumpAllDecimal(t, m, logger)
	require.NoError(t, migrate(t, m, logger))
	return m
}

// TestMigrateDecimalToEpoch is the happy path: a fresh datadir of decimal segments is converted to
// epoch [0,1024),[1024,2048) with indexes, the sub-1024 tail [2048,3000) is written back to the DB,
// the decimal segments are deleted, and a second run is a no-op.
func TestMigrateDecimalToEpoch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	m := createDumpTestKV(t, chain.AllProtocolChanges, migrationChainSize)
	dumpAllDecimal(t, m, logger)

	// Drop a tail block first so we can prove the migration actually writes the tail back.
	dropHeader(t, m, migrationTailBlock)
	require.False(t, headerInDB(t, m, migrationTailBlock))

	require.NoError(t, migrate(t, m, logger))

	// The first epoch tier [0,1024) exists with indexes for all three block types; decimal ones gone.
	requireEpochOnly(t, m)
	for _, typ := range snaptype2.BlockSnapshotTypes {
		for _, idxName := range typ.IdxFileNames(true, 0, 1024) {
			require.FileExists(t, filepath.Join(m.Dirs.Snap, idxName), idxName)
		}
	}

	// Epoch header segment content is correct: word K decodes to header number K.
	d, err := seg.NewDecompressor(filepath.Join(m.Dirs.Snap, snaptype2.Headers.FileInfo(m.Dirs.Snap, true, 0, 1024).Name()))
	require.NoError(t, err)
	defer d.Close()
	require.Equal(t, 1024, d.Count())
	g := d.MakeGetter()
	for k := uint64(0); g.HasNext(); k++ {
		w, _ := g.Next(nil)
		var h types.Header
		require.NoError(t, rlp.DecodeBytes(w[1:], &h))
		require.Equal(t, k, h.Number.Uint64())
	}

	// The tail block was written back to the DB.
	require.True(t, headerInDB(t, m, migrationTailBlock))

	// Idempotent: a second run finds no decimal and is a no-op.
	require.NoError(t, migrate(t, m, logger))
	requireEpochOnly(t, m)
}

// TestEpochMigration_ResumeBeforeTailCommit covers a crash after the epoch segments were produced but
// before the DB tail was committed: the straddling decimal segment (the tail's data source) is still
// on disk. On restart the straddle keeps coverage past tailFrom, so frozenMax(3000) > startBlock
// (2048) and the repack guard does NOT fire — the repack re-collects the tail from the straddle and
// re-commits it. Proven by dropping a tail block before the resume and checking it is restored.
func TestEpochMigration_ResumeBeforeTailCommit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	m := setupMigrated(t, logger)

	// "convert done, tail not yet committed": straddle present again, tail block missing from the DB.
	dumpAllDecimal(t, m, logger)
	dropHeader(t, m, migrationTailBlock)
	require.False(t, headerInDB(t, m, migrationTailBlock))

	require.NoError(t, migrate(t, m, logger))

	// The tail was re-collected from the straddle and written back; the datadir is epoch-only again.
	require.True(t, headerInDB(t, m, migrationTailBlock))
	requireEpochOnly(t, m)
}

// TestEpochMigration_ResumeAfterTailCommit covers a crash after the tail was committed but before the
// straddling decimal segments were deleted: both the DB tail and the straddle are present. The
// straddle still keeps frozenMax > tailFrom, so the repack runs and idempotently re-writes the
// identical tail, then the cleanup removes the straddle. The datadir converges and the tail survives.
func TestEpochMigration_ResumeAfterTailCommit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	m := setupMigrated(t, logger)
	require.True(t, headerInDB(t, m, migrationTailBlock)) // tail already committed by setupMigrated

	// "tail committed, straddle not yet deleted": straddle present again, DB tail left in place.
	dumpAllDecimal(t, m, logger)

	require.NoError(t, migrate(t, m, logger))

	require.True(t, headerInDB(t, m, migrationTailBlock))
	requireEpochOnly(t, m)
}

// TestEpochMigration_ResumeDuringCleanup covers a crash midway through deleting the straddles: the
// tail is already committed, some types' straddles are gone, and at least one type's straddle
// survives. That collapses frozenMax to tailFrom, so the repack guard (startBlock >= frozenMax) skips
// producing/collecting for every type — including the transactions repack, which therefore never
// reads the (now-absent) decimal bodies. This must neither error nor drop the already-committed tail.
// Reproduced by recreating only the transactions straddle.
func TestEpochMigration_ResumeDuringCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	m := setupMigrated(t, logger)
	require.True(t, headerInDB(t, m, migrationTailBlock))

	// Only the transactions straddle ([2000,3000)) survives → frozenMax collapses to tailFrom and the
	// guard fires (headers/bodies have no decimal at all, so their coverage is just the epoch prefix).
	straddleFrom, straddleTo := decimalBounds[len(decimalBounds)-2], decimalBounds[len(decimalBounds)-1]
	dumpDecimalSeg(t, m, straddleFrom, straddleTo, snaptype2.Transactions, 0, logger)

	require.NoError(t, migrate(t, m, logger)) // guard skips the repacks; no "ran out of bodies" error

	require.True(t, headerInDB(t, m, migrationTailBlock)) // the committed tail is untouched
	requireEpochOnly(t, m)                                // the leftover transactions straddle is removed
}

// TestEpochMigration_TailDurableBeforeIndexes pins the write order: the DB tail is committed before
// the epoch indexes are built, so a crash during index building (the longest, most memory-hungry
// phase) leaves the tail already durable instead of forcing it to be re-collected. Index building is
// made to fail by pointing the migration at an unusable tmp dir, which nothing before it needs on a
// resume — the epoch segments already exist, so no compressor runs.
func TestEpochMigration_TailDurableBeforeIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	m := setupMigrated(t, logger)

	dumpAllDecimal(t, m, logger)
	dropHeader(t, m, migrationTailBlock)
	require.False(t, headerInDB(t, m, migrationTailBlock))

	// force the index step to run (and then fail): drop the headers index, and make tmp unusable.
	headersIdx := snaptype2.Headers.IdxFileNames(true, 0, 1024)[0]
	require.NoError(t, dir.RemoveFile(filepath.Join(m.Dirs.Snap, headersIdx)))
	brokenTmp := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(brokenTmp, nil, 0o644))
	dirs := m.Dirs
	dirs.Tmp = brokenTmp

	require.Error(t, freezeblocks.MigrateDecimalToEpoch(m.Ctx, dirs, m.DB, m.ChainConfig, 1, logger))

	require.True(t, headerInDB(t, m, migrationTailBlock),
		"tail must be committed before the indexes are built")
	require.NoFileExists(t, filepath.Join(m.Dirs.Snap, headersIdx))

	// That failure leaves a state this ordering newly makes possible — tail committed, an index
	// missing, straddles still on disk. A resume has to converge from it.
	require.NoError(t, migrate(t, m, logger))
	require.FileExists(t, filepath.Join(m.Dirs.Snap, headersIdx))
	require.True(t, headerInDB(t, m, migrationTailBlock))
	requireEpochOnly(t, m)
}
