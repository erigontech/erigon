package state

import (
	"bytes"
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCursorHeapPriority_RAMOverDBOverFILE(t *testing.T) {
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
