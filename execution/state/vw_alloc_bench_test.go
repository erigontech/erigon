package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
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
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetCode(addr, &VersionedWrite[accounts.Code]{
			WriteHeader: WriteHeader{Address: addr, Path: CodePath},
			Val:         accounts.Code{Bytes: code},
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

// BenchmarkPoolCycle_* exercise the step-2 pool fast path: get from pool,
// fill, insert, then release all on tx-finalize.  Target: 0 allocs/op
// after warmup once the per-type sync.Pool has a steady supply.

func BenchmarkPoolCycle_Balance(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x01})
	v := *uint256.NewInt(0xdeadbeef)
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vw := getVWBalance()
		vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath}
		vw.Val = v
		s.SetBalance(addr, vw)
		s.ReleaseAndReset()
	}
}

func BenchmarkPoolCycle_Storage(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x02})
	key := accounts.InternKey(common.Hash{0x01})
	v := *uint256.NewInt(0xcafef00d)
	var s WriteSet
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vw := getVWStorage()
		vw.WriteHeader = WriteHeader{Address: addr, Path: StoragePath, Key: key}
		vw.Val = v
		s.SetStorage(addr, key, vw)
		s.ReleaseAndReset()
	}
}

// BenchmarkPoolCycle_TxBatch simulates a realistic per-tx mix: 1 sender
// (balance + nonce) + 1 contract (many storage slots), single Reset.  This
// matches sstore-bloated-no-slots's actual write pattern — writes
// concentrate on ~1 contract address, so the per-addr inner-storage-map
// cost amortizes across the full slot batch.
func BenchmarkPoolCycle_TxBatch(b *testing.B) {
	const slotsPerTx = 50
	sender := accounts.InternAddress([20]byte{0xaa})
	contract := accounts.InternAddress([20]byte{0xbb})
	keys := make([]accounts.StorageKey, slotsPerTx)
	for i := 0; i < slotsPerTx; i++ {
		var k common.Hash
		k[0] = byte(i)
		k[1] = byte(i >> 8)
		keys[i] = accounts.InternKey(k)
	}
	bal := *uint256.NewInt(0xdeadbeef)
	slot := *uint256.NewInt(0xcafef00d)
	var s WriteSet

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// One balance + one nonce write per tx (typical sender)
		vwb := getVWBalance()
		vwb.WriteHeader = WriteHeader{Address: sender, Path: BalancePath}
		vwb.Val = bal
		s.SetBalance(sender, vwb)

		vwn := getVWNonce()
		vwn.WriteHeader = WriteHeader{Address: sender, Path: NoncePath}
		vwn.Val = uint64(n)
		s.SetNonce(sender, vwn)

		// Many storage writes to a single contract
		for i := 0; i < slotsPerTx; i++ {
			vws := getVWStorage()
			vws.WriteHeader = WriteHeader{Address: contract, Path: StoragePath, Key: keys[i]}
			vws.Val = slot
			s.SetStorage(contract, keys[i], vws)
		}

		s.ReleaseAndReset()
	}
}

// BenchmarkPoolCycle_Reuse exercises the "second write to same addr" path:
// recordWrite* should reuse the existing entry in place (no alloc, no pool
// op).
func BenchmarkPoolCycle_Reuse(b *testing.B) {
	addr := accounts.InternAddress([20]byte{0x03})
	v1 := *uint256.NewInt(1)
	v2 := *uint256.NewInt(2)
	var s WriteSet
	vw := getVWBalance()
	vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath}
	vw.Val = v1
	s.SetBalance(addr, vw)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if existing, ok := s.GetBalance(addr); ok {
			existing.Val = v2
			continue
		}
		b.Fatal("expected GetBalance hit")
	}
}
