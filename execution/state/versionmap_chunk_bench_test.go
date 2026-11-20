package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkSeedBAL(b *testing.B) {
	for _, n := range []int{64, 128, 256, 512, 1024, 2048, 4096, 16384} {
		bal := make(types.BlockAccessList, n)
		for i := range bal {
			bal[i] = types.AccountChanges{
				Address:        common.Address{17: byte(i >> 16), 18: byte(i >> 8), 19: byte(i)},
				BalanceChanges: []*types.BalanceChange{{Index: uint32(i%7 + 1), Value: *uint256.NewInt(uint64(i + 1))}},
				NonceChanges:   []*types.NonceChange{{Index: uint32(i%5 + 1), Value: uint64(i)}},
				StorageChanges: []types.SlotChanges{{Slot: accounts.InternKey(common.Hash{30: byte(i >> 8), 31: byte(i)}), Changes: []*types.StorageChange{{Index: uint32(i%3 + 1), Value: *uint256.NewInt(uint64(i * 3))}}}},
			}
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for range b.N {
				vm := NewVersionMap(bal)
				vm.SeedAddresses()
			}
		})
	}
}
