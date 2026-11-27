package state

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkMatchesChunks(b *testing.B) {
	defer SetBlockEndChunks(notifyWriteSetsPerWorker, matchesPerWorker)
	r := rand.New(rand.NewPCG(1, 2))
	for _, n := range []int{256, 512, 1024, 1536, 2048, 3072, 4096, 8192, 16384} {
		addrs := make([]accounts.Address, n)
		for i := range addrs {
			var a common.Address
			for j := range a {
				a[j] = byte(r.Uint32())
			}
			addrs[i] = accounts.InternAddress(a)
		}
		build := func() *BlockAccessListBuilder {
			bb := NewBlockAccessListBuilder()
			for i, a := range addrs {
				acc := ensureAccountState(bb.ac, a)
				acc.updateReadBalance(*uint256.NewInt(100))
				acc.applyWriteBalance(*uint256.NewInt(uint64(i)), uint32(i+1))
				acc.applyWriteNonce(uint64(i), uint32(i+1))
			}
			return bb
		}
		want := build().Build()
		bb := build()
		for _, per := range []int{1 << 30, 256, 512, 1024, 2048, 4096} {
			b.Run(fmt.Sprintf("accounts=%d/per=%d", n, per), func(b *testing.B) {
				SetBlockEndChunks(notifyWriteSetsPerWorker, per)
				for b.Loop() {
					if !bb.Matches(want) {
						b.Fatal("mismatch")
					}
				}
			})
		}
	}
}
