package qmtree

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	dbcfg "github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

// openTestDB creates an in-memory MDBX database with qmtree tables.
func openTestDB(t *testing.T) kv.RwDB {
	t.Helper()
	return memdb.NewTestDB(t, dbcfg.ChainDB)
}

// populateEntries writes n entries to the QMTreeEntries table.
func populateEntries(t *testing.T, tx kv.RwTx, from, to uint64) {
	t.Helper()
	for sn := from; sn < to; sn++ {
		var pre, sc, trans common.Hash
		binary.BigEndian.PutUint64(pre[:], sn)
		binary.BigEndian.PutUint64(sc[:], sn*2)
		binary.BigEndian.PutUint64(trans[:], sn*3)
		require.NoError(t, PutEntry(tx, sn, pre, sc, trans))
	}
	require.NoError(t, PutNextSN(tx, to))
}

func TestCollateEntries(t *testing.T) {
	db := openTestDB(t)
	stepSize := uint64(100)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Populate one full step.
	populateEntries(t, tx, 0, stepSize)

	// Collate.
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir, stepSize)
	require.NoError(t, err)
	defer sm.Close()

	require.NoError(t, sm.CollateEntries(context.Background(), tx, 0))
	require.Equal(t, 1, len(sm.entries))
	require.Equal(t, int(stepSize), sm.FrozenEntries())

	// Verify we can read entries from snapshot.
	for sn := uint64(0); sn < stepSize; sn++ {
		pre, sc, trans, found := sm.GetEntryFromSnapshots(sn)
		require.True(t, found, "sn=%d not found", sn)

		var expectedPre, expectedSc, expectedTrans common.Hash
		binary.BigEndian.PutUint64(expectedPre[:], sn)
		binary.BigEndian.PutUint64(expectedSc[:], sn*2)
		binary.BigEndian.PutUint64(expectedTrans[:], sn*3)
		require.Equal(t, expectedPre, pre, "sn=%d pre mismatch", sn)
		require.Equal(t, expectedSc, sc, "sn=%d sc mismatch", sn)
		require.Equal(t, expectedTrans, trans, "sn=%d trans mismatch", sn)
	}

	// Entry beyond the step should not be found.
	_, _, _, found := sm.GetEntryFromSnapshots(stepSize)
	require.False(t, found)
}

func TestPruneEntries(t *testing.T) {
	db := openTestDB(t)
	stepSize := uint64(100)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Populate two steps.
	populateEntries(t, tx, 0, stepSize*2)

	// Collate step 0.
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir, stepSize)
	require.NoError(t, err)
	defer sm.Close()
	require.NoError(t, sm.CollateEntries(context.Background(), tx, 0))

	// Prune step 0.
	pruned, err := sm.PruneEntries(tx, 0)
	require.NoError(t, err)
	require.Equal(t, int(stepSize), pruned)

	// Step 0 entries should be gone from MDBX.
	for sn := uint64(0); sn < stepSize; sn++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], sn)
		val, err := tx.GetOne(kv.TblQMTreeEntries, key[:])
		require.NoError(t, err)
		require.Nil(t, val, "sn=%d should be pruned", sn)
	}

	// Step 1 entries should still exist in MDBX.
	for sn := stepSize; sn < stepSize*2; sn++ {
		pre, sc, trans, err := GetEntry(tx, sn)
		require.NoError(t, err)
		var expectedPre common.Hash
		binary.BigEndian.PutUint64(expectedPre[:], sn)
		require.Equal(t, expectedPre, pre, "sn=%d", sn)
		_ = sc
		_ = trans
	}

	// Step 0 entries should still be readable from snapshot.
	for sn := uint64(0); sn < stepSize; sn++ {
		_, _, _, found := sm.GetEntryFromSnapshots(sn)
		require.True(t, found, "sn=%d should be in snapshot", sn)
	}
}

func TestMergeEntries(t *testing.T) {
	db := openTestDB(t)
	stepSize := uint64(50)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Populate 3 steps.
	populateEntries(t, tx, 0, stepSize*3)

	// Collate each step separately.
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir, stepSize)
	require.NoError(t, err)
	defer sm.Close()

	for step := uint64(0); step < 3; step++ {
		require.NoError(t, sm.CollateEntries(context.Background(), tx, step))
	}
	require.Equal(t, 3, len(sm.entries))

	// Merge steps 0-2 into one file.
	require.NoError(t, sm.MergeEntries(context.Background(), 0, 3))
	require.Equal(t, 1, len(sm.entries))
	require.Equal(t, int(stepSize*3), sm.FrozenEntries())

	// Verify all entries are readable from the merged snapshot.
	for sn := uint64(0); sn < stepSize*3; sn++ {
		pre, _, _, found := sm.GetEntryFromSnapshots(sn)
		require.True(t, found, "sn=%d not found after merge", sn)
		var expectedPre common.Hash
		binary.BigEndian.PutUint64(expectedPre[:], sn)
		require.Equal(t, expectedPre, pre, "sn=%d", sn)
	}
}

func TestCollateAndPrune(t *testing.T) {
	db := openTestDB(t)
	stepSize := uint64(50)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Populate one step + some extra.
	populateEntries(t, tx, 0, stepSize+10)

	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir, stepSize)
	require.NoError(t, err)
	defer sm.Close()

	// CollateAndPrune step 0.
	require.NoError(t, sm.CollateAndPrune(context.Background(), tx, tx, 0))

	// Step 0 entries pruned from MDBX, readable from snapshot.
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 0)
	val, err := tx.GetOne(kv.TblQMTreeEntries, key[:])
	require.NoError(t, err)
	require.Nil(t, val, "sn=0 should be pruned")

	_, _, _, found := sm.GetEntryFromSnapshots(0)
	require.True(t, found, "sn=0 should be in snapshot")

	// Extra entries beyond step 0 still in MDBX.
	pre, _, _, err := GetEntry(tx, stepSize+5)
	require.NoError(t, err)
	var expected common.Hash
	binary.BigEndian.PutUint64(expected[:], stepSize+5)
	require.Equal(t, expected, pre)
}

func TestLoadSnapshots(t *testing.T) {
	db := openTestDB(t)
	stepSize := uint64(50)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	populateEntries(t, tx, 0, stepSize*2)

	dir := t.TempDir()

	// Create snapshots.
	sm1, err := NewSnapshotManager(dir, stepSize)
	require.NoError(t, err)
	require.NoError(t, sm1.CollateEntries(context.Background(), tx, 0))
	require.NoError(t, sm1.CollateEntries(context.Background(), tx, 1))
	sm1.Close()

	// Reload from disk.
	sm2, err := NewSnapshotManager(dir, stepSize)
	require.NoError(t, err)
	defer sm2.Close()

	maxStep, err := sm2.LoadSnapshots()
	require.NoError(t, err)
	require.Equal(t, uint64(2), maxStep)
	require.Equal(t, 2, len(sm2.entries))
	require.Equal(t, int(stepSize*2), sm2.FrozenEntries())

	// Verify lookups work after reload.
	for sn := uint64(0); sn < stepSize*2; sn++ {
		_, _, _, found := sm2.GetEntryFromSnapshots(sn)
		require.True(t, found, "sn=%d", sn)
	}
}
