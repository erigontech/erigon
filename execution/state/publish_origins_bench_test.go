package state

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkPublishOrigins(b *testing.B) {
	for _, slots := range []int{0, 1, 8} {
		b.Run(fmt.Sprintf("addrs=16/slots=%d", slots), func(b *testing.B) {
			ibs := NewWithVersionMap(nil, NewVersionMap(nil))
			acc := &accounts.Account{}
			addrs := make([]accounts.Address, 16)
			for i := range addrs {
				addrs[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1))))
			}
			keys := make([]accounts.StorageKey, slots)
			for i := range keys {
				keys[i] = accounts.InternKey(common.BigToHash(big.NewInt(int64(i))))
			}
			b.ReportAllocs()
			for b.Loop() {
				for _, addr := range addrs {
					ibs.recordAddressOrigin(addr, acc)
					for i, key := range keys {
						ibs.recordStorageOrigin(addr, key, *uint256.NewInt(uint64(i)))
					}
				}
				ibs.PublishOrigins()
			}
		})
	}
}
