package state

import (
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
