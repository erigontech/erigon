// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package memstoredb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// testCfg is a small two-table schema used by the unit tests: one plain,
// one DupSort.
var testCfg = kv.TableCfg{
	"plain": {},
	"dup":   {Flags: kv.DupSort},
}

func newTestDB(t *testing.T) *DB {
	t.Helper()
	db := New("test", testCfg)
	t.Cleanup(db.Close)
	return db
}

// --- Plain table cursor parity ---

func TestPutGetDelete(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		require.NoError(t, tx.Put("plain", []byte("a"), []byte("1")))
		require.NoError(t, tx.Put("plain", []byte("b"), []byte("2")))
		return nil
	}))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, err := tx.GetOne("plain", []byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), v)
		v, err = tx.GetOne("plain", []byte("missing"))
		require.NoError(t, err)
		require.Nil(t, v)
		return nil
	}))
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		require.NoError(t, tx.Delete("plain", []byte("a")))
		return nil
	}))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, err := tx.GetOne("plain", []byte("a"))
		require.NoError(t, err)
		require.Nil(t, v)
		return nil
	}))
}

func TestCursorFirstNextLastPrev(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, k := range []string{"a", "b", "c", "d"} {
			require.NoError(t, tx.Put("plain", []byte(k), []byte(k+"_v")))
		}
		return nil
	}))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		c, err := tx.Cursor("plain")
		require.NoError(t, err)
		defer c.Close()
		k, v, err := c.First()
		require.NoError(t, err)
		require.Equal(t, "a", string(k))
		require.Equal(t, "a_v", string(v))
		k, _, _ = c.Next()
		require.Equal(t, "b", string(k))
		k, _, _ = c.Last()
		require.Equal(t, "d", string(k))
		k, _, _ = c.Prev()
		require.Equal(t, "c", string(k))
		k, _, _ = c.Seek([]byte("bz"))
		require.Equal(t, "c", string(k))
		k, _, _ = c.SeekExact([]byte("b"))
		require.Equal(t, "b", string(k))
		k, _, _ = c.SeekExact([]byte("missing"))
		require.Nil(t, k)
		return nil
	}))
}

// --- DupSort cursor parity ---

func TestDupSortPutSeekIterate(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, v := range []string{"v1", "v2", "v3"} {
			require.NoError(t, tx.Put("dup", []byte("k1"), []byte(v)))
		}
		require.NoError(t, tx.Put("dup", []byte("k2"), []byte("v9")))
		return nil
	}))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		c, err := tx.CursorDupSort("dup")
		require.NoError(t, err)
		defer c.Close()
		// FirstDup expects cursor positioned on a key.
		k, _, _ := c.SeekExact([]byte("k1"))
		require.Equal(t, "k1", string(k))
		v, _ := c.FirstDup()
		require.Equal(t, "v1", string(v))
		v, _ = c.LastDup()
		require.Equal(t, "v3", string(v))
		n, _ := c.CountDuplicates()
		require.EqualValues(t, 3, n)
		// SeekBothExact match / mismatch.
		k2, v2, _ := c.SeekBothExact([]byte("k1"), []byte("v2"))
		require.Equal(t, "k1", string(k2))
		require.Equal(t, "v2", string(v2))
		k2, v2, _ = c.SeekBothExact([]byte("k1"), []byte("missing"))
		require.Nil(t, k2)
		require.Nil(t, v2)
		// SeekBothRange returns the first dup >= value for that key.
		v3, _ := c.SeekBothRange([]byte("k1"), []byte("v2"))
		require.Equal(t, "v2", string(v3))
		// NextDup / NextNoDup.
		c.SeekExact([]byte("k1"))
		c.FirstDup()
		_, vN, _ := c.NextDup()
		require.Equal(t, "v2", string(vN))
		kN, _, _ := c.NextNoDup()
		require.Equal(t, "k2", string(kN))
		// PrevDup / PrevNoDup.
		c.SeekExact([]byte("k1"))
		c.LastDup()
		_, vP, _ := c.PrevDup()
		require.Equal(t, "v2", string(vP))
		return nil
	}))
}

// --- Snapshot isolation: Rollback discards writes; RoTx sees a stable snapshot ---

func TestRollbackDiscards(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put("plain", []byte("k"), []byte("committed"))
	}))
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.Put("plain", []byte("k"), []byte("uncommitted")))
	require.NoError(t, tx.Put("plain", []byte("new"), []byte("staged")))
	tx.Rollback()
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, _ := tx.GetOne("plain", []byte("k"))
		require.Equal(t, "committed", string(v))
		v, _ = tx.GetOne("plain", []byte("new"))
		require.Nil(t, v)
		return nil
	}))
}

func TestRoTxSnapshotStableAcrossConcurrentCommit(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put("plain", []byte("k"), []byte("v1"))
	}))
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	// Concurrent committed write should not be visible to the RoTx snapshot.
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put("plain", []byte("k"), []byte("v2"))
	}))
	v, _ := roTx.GetOne("plain", []byte("k"))
	require.Equal(t, "v1", string(v))
	// New BeginRo sees the new value.
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, _ := tx.GetOne("plain", []byte("k"))
		require.Equal(t, "v2", string(v))
		return nil
	}))
}

// TestRwTxDeleteIsolatedFromRoTx is a regression test for a snapshot-isolation
// bug: tx.Delete used to mutate master's shared table when called on a RwTx
// that hadn't yet COW-cloned that table (no prior Put on it), causing
// concurrent RoTx readers to observe the deletion mid-tx and breaking
// rollback semantics. Same scenario for ClearTable.
func TestRwTxDeleteIsolatedFromRoTx(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		require.NoError(t, tx.Put("plain", []byte("a"), []byte("1")))
		require.NoError(t, tx.Put("plain", []byte("b"), []byte("2")))
		return nil
	}))
	// Concurrent RoTx open BEFORE the RwTx-with-Delete starts.
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// RwTx that ONLY calls Delete (no Put first → no implicit COW).
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	require.NoError(t, rwTx.Delete("plain", []byte("a")))
	// RoTx must still see "a" — the deletion is uncommitted.
	v, err := roTx.GetOne("plain", []byte("a"))
	require.NoError(t, err)
	require.Equal(t, "1", string(v), "RoTx must not see RwTx's uncommitted Delete")
	// Rollback the RwTx.
	rwTx.Rollback()
	// After rollback master must still have "a".
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, _ := tx.GetOne("plain", []byte("a"))
		require.Equal(t, "1", string(v), "Rollback must undo the Delete")
		return nil
	}))
}

func TestRwTxClearTableIsolatedFromRoTx(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		require.NoError(t, tx.Put("plain", []byte("a"), []byte("1")))
		require.NoError(t, tx.Put("plain", []byte("b"), []byte("2")))
		return nil
	}))
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	require.NoError(t, rwTx.ClearTable("plain"))
	// RoTx must still see "a" / "b".
	v, err := roTx.GetOne("plain", []byte("a"))
	require.NoError(t, err)
	require.Equal(t, "1", string(v), "RoTx must not see RwTx's uncommitted ClearTable")
	rwTx.Rollback()
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, _ := tx.GetOne("plain", []byte("a"))
		require.Equal(t, "1", string(v), "Rollback must undo the ClearTable")
		return nil
	}))
}

// --- Sequence helpers ---

func TestSequence(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		base, err := tx.IncrementSequence("seq", 10)
		require.NoError(t, err)
		require.EqualValues(t, 0, base)
		base, err = tx.IncrementSequence("seq", 5)
		require.NoError(t, err)
		require.EqualValues(t, 10, base)
		cur, err := tx.ReadSequence("seq")
		require.NoError(t, err)
		require.EqualValues(t, 15, cur)
		require.NoError(t, tx.ResetSequence("seq", 100))
		cur, err = tx.ReadSequence("seq")
		require.NoError(t, err)
		require.EqualValues(t, 100, cur)
		return nil
	}))
}

// TestSaveLoadRoundTrip writes a populated DB to disk, closes it, opens a
// fresh DB at the same path, and verifies all (k, v) pairs and sequences
// round-trip exactly. Exercises the on-Close persistence path required by
// multi-process CLI tooling (erigon init / import / daemon).
func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "chaindata")
	cfg := kv.TableCfg{
		"plain": {},
		"dup":   {Flags: kv.DupSort},
	}
	ctx := context.Background()

	db := OpenForPath(path, "test", cfg)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		require.NoError(t, tx.Put("plain", []byte("a"), []byte("1")))
		require.NoError(t, tx.Put("plain", []byte("b"), []byte("2")))
		for _, v := range []string{"x", "y", "z"} {
			require.NoError(t, tx.Put("dup", []byte("k"), []byte(v)))
		}
		_, err := tx.IncrementSequence("seq1", 42)
		require.NoError(t, err)
		return nil
	}))
	db.Close()
	WipePath(path)

	db2 := OpenForPath(path, "test", cfg)
	t.Cleanup(db2.Close)
	require.NoError(t, db2.View(ctx, func(tx kv.Tx) error {
		v, _ := tx.GetOne("plain", []byte("a"))
		require.Equal(t, "1", string(v))
		v, _ = tx.GetOne("plain", []byte("b"))
		require.Equal(t, "2", string(v))
		seq, _ := tx.ReadSequence("seq1")
		require.EqualValues(t, 42, seq)
		c, err := tx.CursorDupSort("dup")
		require.NoError(t, err)
		defer c.Close()
		_, v, _ = c.SeekExact([]byte("k"))
		require.Equal(t, "x", string(v))
		n, _ := c.CountDuplicates()
		require.EqualValues(t, 3, n)
		return nil
	}))
}
