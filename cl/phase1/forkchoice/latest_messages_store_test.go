package forkchoice

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// A validator's stored vote must survive any number of other validators'
// distinct messages (more than fit in a 16-bit id space).
func TestLatestMessagesStoreSetSurvivesDistinctMessageChurn(t *testing.T) {
	l := newLatestMessagesStore(2)

	pinned := LatestMessage{Epoch: 1, Root: common.Hash{0x01}}
	l.set(0, pinned)

	got, ok := l.get(0)
	require.True(t, ok)
	require.Equal(t, pinned, got)

	for i := 0; i < math.MaxUint16+2; i++ {
		l.set(1, LatestMessage{Epoch: uint64(i + 2), Root: common.Hash{0xff}, Slot: uint64(i)})
	}

	got, ok = l.get(0)
	require.True(t, ok)
	require.Equal(t, pinned, got)
}

// Growing the store past its capacity must preserve previously stored votes.
func TestLatestMessagesStoreGrowthPreservesEntries(t *testing.T) {
	l := newLatestMessagesStore(2)

	pinned := LatestMessage{Epoch: 1, Root: common.Hash{0x01}}
	l.set(0, pinned)
	l.set(100, LatestMessage{Epoch: 2, Root: common.Hash{0x02}})

	require.Equal(t, 101, l.latestMessagesCount())
	got, ok := l.get(0)
	require.True(t, ok)
	require.Equal(t, pinned, got)

	_, ok = l.get(50)
	require.False(t, ok)
}

// One validator updates its vote while many other validators hold distinct votes.
func BenchmarkLatestMessagesStoreSetWithPinnedMessages(b *testing.B) {
	l := newLatestMessagesStore(100_000)
	for i := 0; i < 10_000; i++ {
		l.set(i, LatestMessage{Epoch: uint64(i + 1), Root: common.Hash{byte(i), byte(i >> 8), 0x01}, Slot: uint64(i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.set(50_000, LatestMessage{Epoch: uint64(i + 20_000), Root: common.Hash{0xaa}, Slot: uint64(i)})
	}
}
