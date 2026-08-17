package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkVersionedReadGetters(b *testing.B) {
	_, tx, domains := NewTestRwTx(b)
	_ = tx
	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsGetter(tx))

	addr := accounts.InternAddress([20]byte{0x01})
	key := accounts.InternKey([32]byte{0x01})

	v0 := Version{TxIndex: 0, Incarnation: 1}
	mvhm.WriteNonce(addr, v0, uint64(42), true)
	mvhm.WriteBalance(addr, v0, *uint256.NewInt(100), true)
	mvhm.WriteStorage(addr, key, v0, *uint256.NewInt(123), true)

	s := NewWithVersionMap(reader, mvhm)
	defer s.Close()
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

// Isolates steady-state warm-read cost: after the first Empty+GetCodeHash the reads hit the read-once fast path.
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
	_, _ = s.Empty(addr)
	_, _ = s.GetCodeHash(addr)

	b.ReportAllocs()
	for b.Loop() {
		_, _ = s.Empty(addr)
		_, _ = s.GetCodeHash(addr)
	}
}
