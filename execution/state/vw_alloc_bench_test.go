package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
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
	for i := range slotsPerTx {
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
		for i := range slotsPerTx {
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

// BenchmarkArenaOverflowCycle records more storage writes than the slabs can
// hold and resets, so every cycle pays for the cells past vwMaxCells.
func BenchmarkArenaOverflowCycle(b *testing.B) {
	const slots = 4096
	contract := accounts.InternAddress([20]byte{0x05})
	keys := make([]accounts.StorageKey, slots)
	for i := range slots {
		var k common.Hash
		k[0] = byte(i)
		k[1] = byte(i >> 8)
		keys[i] = accounts.InternKey(k)
	}
	val := *uint256.NewInt(0xcafef00d)
	var ws WriteSet
	ws.UseArena()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for i := range slots {
			vw := ws.newVWStorage()
			vw.WriteHeader = WriteHeader{Address: contract, Path: StoragePath, Key: keys[i]}
			vw.Val = val
			ws.SetStorage(contract, keys[i], vw)
		}
		ws.ReleaseAndReset()
	}
}

// BenchmarkFreshSet writes three paths on a set that is never reused, the shape
// of a single eth_call: the arena arm pays for a slab per path it touches with
// nothing to amortize them.
func BenchmarkFreshSet(b *testing.B) {
	for _, arena := range []bool{false, true} {
		name := "pooled"
		if arena {
			name = "arena"
		}
		b.Run(name, func(b *testing.B) { benchFreshSet(b, arena) })
	}
}

func benchFreshSet(b *testing.B, arena bool) {
	addr := accounts.InternAddress([20]byte{0x06})
	key := accounts.InternKey(common.Hash{0x07})
	bal := *uint256.NewInt(0xdeadbeef)
	val := *uint256.NewInt(0xcafef00d)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var ws WriteSet
		if arena {
			ws.UseArena()
		}

		vwn := ws.newVWNonce()
		vwn.WriteHeader = WriteHeader{Address: addr, Path: NoncePath}
		vwn.Val = 1
		ws.SetNonce(addr, vwn)

		vwb := ws.newVWBalance()
		vwb.WriteHeader = WriteHeader{Address: addr, Path: BalancePath}
		vwb.Val = bal
		ws.SetBalance(addr, vwb)

		vws := ws.newVWStorage()
		vws.WriteHeader = WriteHeader{Address: addr, Path: StoragePath, Key: key}
		vws.Val = val
		ws.SetStorage(addr, key, vws)

		ws.ReleaseAndReset()
	}
}

// BenchmarkFreshSetWrites sweeps how many storage cells a one-shot set records,
// to find where its own slabs start beating the shared pools.
func BenchmarkFreshSetWrites(b *testing.B) {
	for _, writes := range []int{1, 4, 16, 64, 256, 1024} {
		keys := make([]accounts.StorageKey, writes)
		for i := range keys {
			var k common.Hash
			k[0] = byte(i)
			k[1] = byte(i >> 8)
			keys[i] = accounts.InternKey(k)
		}
		for _, arena := range []bool{false, true} {
			name := fmt.Sprintf("w%d/pooled", writes)
			if arena {
				name = fmt.Sprintf("w%d/arena", writes)
			}
			b.Run(name, func(b *testing.B) { benchFreshSetWrites(b, arena, keys) })
		}
	}
}

func benchFreshSetWrites(b *testing.B, arena bool, keys []accounts.StorageKey) {
	addr := accounts.InternAddress([20]byte{0x08})
	val := *uint256.NewInt(0xcafef00d)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var ws WriteSet
		if arena {
			ws.UseArena()
		}
		for _, k := range keys {
			vw := ws.newVWStorage()
			vw.WriteHeader = WriteHeader{Address: addr, Path: StoragePath, Key: k}
			vw.Val = val
			ws.SetStorage(addr, k, vw)
		}
		ws.ReleaseAndReset()
	}
}
