package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The list holds a block only between finishing and committing to the shared
// domain — pushed at the head, removed from the tail in block order — so its
// length is the exec-ahead window, never the whole chain.

func TestPrevBlockList_BeforeIsEarlierBlocksOldestFirst(t *testing.T) {
	t.Parallel()
	addr := getAddress(1)
	l := NewPrevBlockList()
	l.PushHead(10, 10, mapWithBalance(addr, 10))
	l.PushHead(11, 11, mapWithBalance(addr, 11))
	l.PushHead(12, 12, mapWithBalance(addr, 12))

	// A reader for block 12 sees 10 and 11 (not 12), oldest→newest.
	got := l.Before(12)
	require.Len(t, got, 2)
	b0, _, _ := got[0].ReadBalance(addr, finalTxIdx)
	b1, _, _ := got[1].ReadBalance(addr, finalTxIdx)
	assert.Equal(t, uint64(10), b0.Uint64(), "oldest (tail) first")
	assert.Equal(t, uint64(11), b1.Uint64())
}

func TestPrevBlockList_RemoveTailDropsOldest(t *testing.T) {
	t.Parallel()
	addr := getAddress(2)
	l := NewPrevBlockList()
	l.PushHead(10, 10, mapWithBalance(addr, 10))
	l.PushHead(11, 11, mapWithBalance(addr, 11))

	l.RemoveTail() // block 10 committed to the shared domain
	got := l.Before(12)
	require.Len(t, got, 1)
	b, _, _ := got[0].ReadBalance(addr, finalTxIdx)
	assert.Equal(t, uint64(11), b.Uint64(), "only block 11 remains")

	l.RemoveTail()
	assert.Nil(t, l.Before(12))
	l.RemoveTail() // empty list: no-op, no panic
}

func TestPrevBlockList_EmptyAndNoEarlier(t *testing.T) {
	t.Parallel()
	l := NewPrevBlockList()
	assert.Nil(t, l.Before(5))
	l.PushHead(10, 10, NewVersionMap(nil))
	assert.Nil(t, l.Before(10), "no block earlier than 10")
}
