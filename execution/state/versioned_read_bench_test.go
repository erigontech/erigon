package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkVersionedReadGetters measures per-call allocs for GetNonce/GetBalance/GetState
// when the IBS is backed by a VersionMap (parallel execution path).
func BenchmarkVersionedReadGetters(b *testing.B) {
	_, tx, domains := NewTestRwTx(b)
	_ = tx
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress([20]byte{0x01})
	key := accounts.InternKey([32]byte{0x01})

	// Seed version map: tx 0 wrote nonce, balance, storage
	v0 := Version{TxIndex: 0, Incarnation: 1}
	mvhm.WriteNonce(addr, v0, uint64(42), true)
	mvhm.WriteBalance(addr, v0, *uint256.NewInt(100), true)
	mvhm.WriteStorage(addr, key, v0, *uint256.NewInt(123), true)

	// tx 1 reads → hits MapRead path inside versionedRead
	s := NewWithVersionMap(reader, mvhm)
	s.txIndex = 1

	b.Run("GetNonce", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = s.GetNonce(addr)
		}
	})

	b.Run("GetBalance", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = s.GetBalance(addr)
		}
	})

	b.Run("GetState", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = s.GetState(addr, key)
		}
	})
}

// BenchmarkWarmExtCodeHash measures the warm EXTCODEHASH opcode sequence
// (Empty + GetCodeHash) on the noMaterialize/versionMap path — the hot path of
// the warm-extcodehash perf cell. After the first iteration the reads hit the
// read-once fast-path, so this isolates the steady-state warm-read cost + allocs.
func BenchmarkWarmExtCodeHashSeq(b *testing.B) {
	_, tx, domains := NewTestRwTx(b)
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01}
	ch := accounts.InternCodeHash(crypto.Keccak256Hash(code))
	v0 := Version{TxIndex: 0, Incarnation: 1}
	mvhm.WriteAddress(addr, v0, func() *accounts.Account { a := accounts.NewAccount(); a.Nonce = 1; a.CodeHash = ch; return &a }(), true)
	mvhm.WriteNonce(addr, v0, uint64(1), true)
	mvhm.WriteBalance(addr, v0, uint256.Int{}, true)
	mvhm.WriteCodeHash(addr, v0, ch, true)

	s := NewWithVersionMap(reader, mvhm)
	s.SetNoMaterialize(true)
	s.txIndex = 1
	// warm the read-set (first EXTCODEHASH)
	_, _ = s.Empty(addr)
	_, _ = s.GetCodeHash(addr)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = s.Empty(addr)
		_, _ = s.GetCodeHash(addr)
	}
}
