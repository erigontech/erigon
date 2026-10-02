package forkchoice

import (
	"testing"

	"github.com/erigontech/erigon/common"
)

// One validator updates its vote while many other validators hold distinct votes.
func BenchmarkLatestMessagesStoreSetWithPinnedMessages(b *testing.B) {
	l := newLatestMessagesStore(100_000)
	for i := range 10_000 {
		l.set(i, LatestMessage{Epoch: uint64(i + 1), Root: common.Hash{byte(i), byte(i >> 8), 0x01}, Slot: uint64(i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.set(50_000, LatestMessage{Epoch: uint64(i + 20_000), Root: common.Hash{0xaa}, Slot: uint64(i)})
	}
}
