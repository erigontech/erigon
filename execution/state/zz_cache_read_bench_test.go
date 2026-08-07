package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func benchAccount() *accounts.Account {
	acc := accounts.NewAccount()
	acc.Nonce = 42
	acc.Balance = *uint256.NewInt(1e18)
	acc.Incarnation = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x00}))
	return &acc
}

// BenchmarkCachedReaderAccountRead prices one apply-loop account read that hits
// the block state cache, on each of the cache's two paths.
func BenchmarkCachedReaderAccountRead(b *testing.B) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	acc := benchAccount()

	b.Run("committed", func(b *testing.B) {
		cache := NewBlockStateCache()
		cache.PutCommittedAccount(addr, acc)
		r := NewCurrentCachedReaderV3(nil, cache)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := r.ReadAccountData(addr)
			if err != nil || got == nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("current", func(b *testing.B) {
		cache := NewBlockStateCache()
		cache.WriteAccount(addr, accounts.SerialiseV3(acc), 1)
		r := NewCurrentCachedReaderV3(nil, cache)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := r.ReadAccountData(addr)
			if err != nil || got == nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("codec_only", func(b *testing.B) {
		enc := accounts.SerialiseV3(acc)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out accounts.Account
			if err := accounts.DeserialiseV3(&out, enc); err != nil {
				b.Fatal(err)
			}
		}
	})
}
