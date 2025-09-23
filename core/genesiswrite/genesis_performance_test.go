package genesiswrite

import (
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkSortedAllocKeys(b *testing.B) {
	alloc := make(types.GenesisAlloc)
	for i := 0; i < 1000000; i++ {
		addr := common.BytesToAddress([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		alloc[addr] = types.GenesisAccount{
			Balance: big.NewInt(1000000),
			Nonce:   0,
		}
	}

	b.Run("Origin:StringKeysAndToAddress", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			keys := sortedAllocKeys(alloc)
			for _, key := range keys {
				_ = common.BytesToAddress([]byte(key))
			}
		}
	})

	b.Run("Optimized:AddressKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = sortedAllocAddresses(alloc)
		}
	})
}
