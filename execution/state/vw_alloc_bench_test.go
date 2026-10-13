package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkSetBalance exercises the typed write-set Set path that the
// IBS hot recordWriteBalance helper uses on every BalancePath emit.
// Target: zero allocs/op (struct allocation aside).
func BenchmarkSetBalance(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x01})
	v := *uint256.NewInt(0xdeadbeef)
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetBalance(addr, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: BalancePath},
			Val:         v,
		})
	}
}

func BenchmarkSetNonce(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x02})
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetNonce(addr, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: addr, Path: NoncePath},
			Val:         uint64(i),
		})
	}
}

func BenchmarkSetCode(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x03})
	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52}
	codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetCode(addr, &VersionedWrite[accounts.Code]{
			WriteHeader: WriteHeader{Address: addr, Path: CodePath},
			Val:         accounts.Code{Hash: codeHash, Bytes: code},
		})
	}
}

func BenchmarkSetCodeHash(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x04})
	var h accounts.CodeHash
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{
			WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath},
			Val:         h,
		})
	}
}

func BenchmarkSetAddress(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x05})
	a := &accounts.Account{}
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetAddress(addr, &VersionedWrite[*accounts.Account]{
			WriteHeader: WriteHeader{Address: addr, Path: AddressPath},
			Val:         a,
		})
	}
}
