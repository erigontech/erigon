package state

import (
	"testing"

	"github.com/holiman/uint256"

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
	mvhm.Write(addr, NoncePath, accounts.NilKey, v0, uint64(42), true)
	mvhm.Write(addr, BalancePath, accounts.NilKey, v0, *uint256.NewInt(100), true)
	mvhm.Write(addr, StoragePath, key, v0, *uint256.NewInt(123), true)

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
