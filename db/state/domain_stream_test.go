package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestCursorHeapPriority_RAMOverDBOverFILE(t *testing.T) {
	t.Parallel()
	// When the same key exists in RAM, DB, and FILE with equal endTxNum,
	// RAM must win (highest priority), then DB, then FILE.
	// This prevents IteratePrefix from returning stale DB/FILE values
	// when the key has been updated in the uncommitted RAM batch.
	items := []*CursorItem{
		{t: FILE_CURSOR, key: []byte("key1"), val: []byte("file_val"), endTxNum: 100, reverse: true},
		{t: DB_CURSOR, key: []byte("key1"), val: []byte("db_val"), endTxNum: 100, reverse: true},
		{t: RAM_CURSOR, key: []byte("key1"), val: []byte("ram_val"), endTxNum: 100, reverse: true},
	}

	var h CursorHeap
	heap.Init(&h)
	for _, item := range items {
		heap.Push(&h, item)
	}

	top := heap.Pop(&h).(*CursorItem)
	assert.Equal(t, RAM_CURSOR, top.t, "RAM cursor must have highest priority")
	assert.Equal(t, []byte("ram_val"), top.val)

	top = heap.Pop(&h).(*CursorItem)
	assert.Equal(t, DB_CURSOR, top.t, "DB cursor must have second priority")

	top = heap.Pop(&h).(*CursorItem)
	assert.Equal(t, FILE_CURSOR, top.t, "FILE cursor must have lowest priority")
}

func TestCursorHeapPriority_MaxUint64TieBreak(t *testing.T) {
	t.Parallel()
	// Reproduces the exact scenario from #20246: RAM and DB both use
	// math.MaxUint64 as endTxNum in debugIteratePrefixLatest.
	// RAM must win so DomainDelPrefix picks up the current uncommitted value.
	var h CursorHeap
	heap.Init(&h)
	heap.Push(&h, &CursorItem{t: DB_CURSOR, key: []byte("key1"), val: []byte("0f"), endTxNum: ^uint64(0), reverse: true})
	heap.Push(&h, &CursorItem{t: RAM_CURSOR, key: []byte("key1"), val: []byte("15"), endTxNum: ^uint64(0), reverse: true})

	top := heap.Pop(&h).(*CursorItem)
	assert.Equal(t, RAM_CURSOR, top.t, "RAM must beat DB when endTxNum is equal")
	assert.Equal(t, []byte("15"), top.val)
}

func TestCursorHeapPriority_EndTxNumStillWins(t *testing.T) {
	t.Parallel()
	// Higher endTxNum should still beat lower endTxNum regardless of cursor type.
	var h CursorHeap
	heap.Init(&h)
	heap.Push(&h, &CursorItem{t: RAM_CURSOR, key: []byte("key1"), val: []byte("old"), endTxNum: 50, reverse: true})
	heap.Push(&h, &CursorItem{t: FILE_CURSOR, key: []byte("key1"), val: []byte("new"), endTxNum: 100, reverse: true})

	top := heap.Pop(&h).(*CursorItem)
	assert.Equal(t, FILE_CURSOR, top.t, "higher endTxNum wins regardless of cursor type")
	assert.Equal(t, []byte("new"), top.val)
}

func TestCursorHeapMergeLoop_RAMOverridesDB(t *testing.T) {
	t.Parallel()
	// Simulates the merge loop in debugIteratePrefixLatest where the same key
	// exists in both RAM and DB with endTxNum=MaxUint64. The loop takes lastVal
	// from the heap top, then pops all items with that key. RAM must be the top
	// so lastVal comes from RAM, not DB.
	//
	// This is the exact scenario that caused #20246: DomainDelPrefix iterated
	// storage keys, and the DB value (stale) was picked instead of the RAM value
	// (current uncommitted write).
	prefix := []byte("addr")
	key1 := []byte("addr" + "slot09")
	key2 := []byte("addr" + "slotFF")

	var h CursorHeap
	heap.Init(&h)

	// RAM has key1=0x15 (current), key2=0xAA
	// DB has key1=0x0f (stale), key2=0xBB
	// Simulating: RAM_CURSOR items are pushed first, then DB_CURSOR items,
	// but insertion order shouldn't matter — heap ordering should handle it.
	heap.Push(&h, &CursorItem{t: DB_CURSOR, key: key1, val: []byte{0x0f}, endTxNum: ^uint64(0), reverse: true})
	heap.Push(&h, &CursorItem{t: RAM_CURSOR, key: key1, val: []byte{0x15}, endTxNum: ^uint64(0), reverse: true})
	heap.Push(&h, &CursorItem{t: DB_CURSOR, key: key2, val: []byte{0xBB}, endTxNum: ^uint64(0), reverse: true})
	heap.Push(&h, &CursorItem{t: RAM_CURSOR, key: key2, val: []byte{0xAA}, endTxNum: ^uint64(0), reverse: true})

	// Run the same merge loop as debugIteratePrefixLatest
	type result struct {
		key, val []byte
	}
	var results []result

	for h.Len() > 0 {
		lastKey := append([]byte{}, h[0].key...)
		lastVal := append([]byte{}, h[0].val...)

		if !bytes.HasPrefix(lastKey, prefix) {
			break
		}

		// Pop all items with this key
		for h.Len() > 0 && bytes.Equal(h[0].key, lastKey) {
			heap.Pop(&h)
		}

		if len(lastVal) > 0 {
			results = append(results, result{lastKey, lastVal})
		}
	}

	assert.Len(t, results, 2)
	// key1: RAM value 0x15 must win over DB value 0x0f
	assert.Equal(t, key1, results[0].key)
	assert.Equal(t, []byte{0x15}, results[0].val, "RAM value must override DB for key1")
	// key2: RAM value 0xAA must win over DB value 0xBB
	assert.Equal(t, key2, results[1].key)
	assert.Equal(t, []byte{0xAA}, results[1].val, "RAM value must override DB for key2")
}

// TestDomain_IteratePrefix_PrefersFilesOverDB verifies that debugIteratePrefixLatest
// skips DB entries whose step falls within the file range, so file values are
// returned instead of stale DB data.
//
// After a partial (lexicographic) prune, the DB may retain stale entries with
// steps already covered by files. The fix (#20355) skips DB entries where
// step.ToTxNum < files.EndTxNum(), so they are never pushed into the heap.
func TestDomain_IteratePrefix_PrefersFilesOverDB(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := t.Context()
	require := require.New(t)

	stepSize := uint64(16)
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, stepSize, logger)

	// Write a key at step 0 (txNum=1) with V0, then at step 1 (txNum=17) with V1.
	var key [8]byte
	key[0] = 0x01
	key[7] = 0x01
	prefix := key[:4]

	var v0, v1 [8]byte
	v0[7] = 0xAA
	v1[7] = 0xBB

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	dt := d.beginForTests()
	writer := dt.NewWriter()

	require.NoError(writer.PutWithPrev(key[:], v0[:], 1, nil))
	require.NoError(writer.Flush(ctx, tx))

	require.NoError(writer.PutWithPrev(key[:], v1[:], 17, v0[:]))
	require.NoError(writer.Flush(ctx, tx))

	writer.Close()
	dt.Close()
	require.NoError(tx.Commit())

	// Build files for steps 0 and 1 without pruning.
	tx, err = db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	collateAndMergeOnce(t, d, tx, kv.Step(0), false)
	collateAndMergeOnce(t, d, tx, kv.Step(1), false)
	require.NoError(tx.Commit())

	// Files now cover the key with latest value V1.
	dt = d.beginForTests()
	require.Greater(dt.files.EndTxNum(), uint64(0), "files should exist")
	dt.Close()

	// Delete step 1 dup from DB, leaving only the stale step 0 entry (V0).
	// This simulates the state after a partial lexicographic prune where the
	// latest dup was removed but a stale older dup remained.
	tx, err = db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	c, err := tx.RwCursorDupSort(d.ValuesTable)
	require.NoError(err)
	defer c.Close()       //nolint:gocritic
	var step1Dup [16]byte // [^step1 (8 bytes) | V1 (8 bytes)]
	binary.BigEndian.PutUint64(step1Dup[:8], ^uint64(1))
	copy(step1Dup[8:], v1[:])
	require.NoError(c.DeleteExact(key[:], step1Dup[:]))
	require.NoError(tx.Commit())

	// debugIteratePrefixLatest should return V1 (from files),
	// not V0 (from the stale DB entry at step 0).
	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	dt = d.beginForTests()
	defer dt.Close()

	var gotVal []byte
	var ramIter btree2.MapIter[string, []dataWithTxNum]
	err = dt.debugIteratePrefixLatest(prefix, ramIter, func(k, v []byte) (bool, error) {
		if bytes.Equal(k, key[:]) {
			gotVal = common.Copy(v)
		}
		return true, nil
	}, roTx)
	require.NoError(err)

	require.Equal(v1[:], gotVal,
		"file value (V1) must win over stale DB value (V0) when DB step is within file range")
}

// TestDomainLatestIterFile_PrefersFilesOverDB verifies that DomainLatestIterFile
// (used by DebugRangeLatest) skips DB entries whose step falls within the file
// range, so file values are returned instead of stale DB data.
//
// This is the DomainLatestIterFile counterpart to
// TestDomain_IteratePrefix_PrefersFilesOverDB which tests debugIteratePrefixLatest.
func TestDomainLatestIterFile_PrefersFilesOverDB(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()
	require := require.New(t)

	stepSize := uint64(16)
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, stepSize, logger)

	// Write a key at step 0 (txNum=1) with V0, then at step 1 (txNum=17) with V1.
	var key [8]byte
	key[0] = 0x01
	key[7] = 0x01

	var v0, v1 [8]byte
	v0[7] = 0xAA
	v1[7] = 0xBB

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	dt := d.beginForTests()
	writer := dt.NewWriter()

	require.NoError(writer.PutWithPrev(key[:], v0[:], 1, nil))
	require.NoError(writer.Flush(ctx, tx))

	require.NoError(writer.PutWithPrev(key[:], v1[:], 17, v0[:]))
	require.NoError(writer.Flush(ctx, tx))

	writer.Close()
	dt.Close()
	require.NoError(tx.Commit())

	// Build files for steps 0 and 1 without pruning.
	tx, err = db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	collateAndMergeOnce(t, d, tx, kv.Step(0), false)
	collateAndMergeOnce(t, d, tx, kv.Step(1), false)
	require.NoError(tx.Commit())

	// Files now cover the key with latest value V1.
	dt = d.beginForTests()
	require.Greater(dt.files.EndTxNum(), uint64(0), "files should exist")
	dt.Close()

	// Delete step 1 dup from DB, leaving only the stale step 0 entry (V0).
	// This simulates the state after a partial lexicographic prune where the
	// latest dup was removed but a stale older dup remained.
	tx, err = db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	c, err := tx.RwCursorDupSort(d.ValuesTable)
	require.NoError(err)
	defer c.Close()       //nolint:gocritic
	var step1Dup [16]byte // [^step1 (8 bytes) | V1 (8 bytes)]
	binary.BigEndian.PutUint64(step1Dup[:8], ^uint64(1))
	copy(step1Dup[8:], v1[:])
	require.NoError(c.DeleteExact(key[:], step1Dup[:]))
	require.NoError(tx.Commit())

	// DebugRangeLatest (which uses DomainLatestIterFile) should return V1
	// (from files), not V0 (from the stale DB entry at step 0).
	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	dt = d.beginForTests()
	defer dt.Close()

	iter, err := dt.DebugRangeLatest(roTx, nil, nil, kv.Unlim)
	require.NoError(err)
	defer iter.Close()

	var gotVal []byte
	for iter.HasNext() {
		k, v, err := iter.Next()
		require.NoError(err)
		if bytes.Equal(k, key[:]) {
			gotVal = common.Copy(v)
		}
	}

	require.Equal(v1[:], gotVal,
		"file value (V1) must win over stale DB value (V0) when DB step is within file range")
}

// TestDomainLatestIterFile_PrefersFilesOverDB_LargeValues is the LargeValues
// counterpart to TestDomainLatestIterFile_PrefersFilesOverDB.  It uses
// CodeDomain (LargeValues: true) so that both initCursorOnDB branches and both
// advanceInFiles DB_CURSOR branches are exercised.
//
// For LargeValues domains the DB key layout is:
//
//	key = userKey || ^step (8 bytes)
//	value = raw value
//
// (as opposed to DupSort domains where key = userKey, dup = ^step || value).
func TestDomainLatestIterFile_PrefersFilesOverDB_LargeValues(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()
	require := require.New(t)

	stepSize := uint64(16)
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.CodeDomain, stepSize, logger)

	// Write a key at step 0 (txNum=1) with V0, then at step 1 (txNum=17) with V1.
	var key [8]byte
	key[0] = 0x01
	key[7] = 0x01

	var v0, v1 [8]byte
	v0[7] = 0xAA
	v1[7] = 0xBB

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	dt := d.beginForTests()
	writer := dt.NewWriter()

	require.NoError(writer.PutWithPrev(key[:], v0[:], 1, nil))
	require.NoError(writer.Flush(ctx, tx))

	require.NoError(writer.PutWithPrev(key[:], v1[:], 17, v0[:]))
	require.NoError(writer.Flush(ctx, tx))

	writer.Close()
	dt.Close()
	require.NoError(tx.Commit())

	// Build files for steps 0 and 1 without pruning.
	tx, err = db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	collateAndMergeOnce(t, d, tx, kv.Step(0), false)
	collateAndMergeOnce(t, d, tx, kv.Step(1), false)
	require.NoError(tx.Commit())

	// Files now cover the key with latest value V1.
	dt = d.beginForTests()
	require.Greater(dt.files.EndTxNum(), uint64(0), "files should exist")
	dt.Close()

	// Delete step 1 entry from DB, leaving only the stale step 0 entry (V0).
	// For LargeValues the DB key is userKey || ^step (8 bytes).
	tx, err = db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	var step1Key [16]byte // [userKey (8 bytes) || ^step1 (8 bytes)]
	copy(step1Key[:8], key[:])
	binary.BigEndian.PutUint64(step1Key[8:], ^uint64(1))
	require.NoError(tx.Delete(d.ValuesTable, step1Key[:]))
	require.NoError(tx.Commit())

	// DebugRangeLatest (which uses DomainLatestIterFile) should return V1
	// (from files), not V0 (from the stale DB entry at step 0).
	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	dt = d.beginForTests()
	defer dt.Close()

	iter2, err := dt.DebugRangeLatest(roTx, nil, nil, kv.Unlim)
	require.NoError(err)
	defer iter2.Close()

	var gotVal []byte
	for iter2.HasNext() {
		k, v, err := iter2.Next()
		require.NoError(err)
		if bytes.Equal(k, key[:]) {
			gotVal = common.Copy(v)
		}
	}

	require.Equal(v1[:], gotVal,
		"file value (V1) must win over stale DB value (V0) when DB step is within file range")
}
