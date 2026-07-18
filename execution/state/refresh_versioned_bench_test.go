package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// benchRefreshAddrs builds nAddrs deterministic addresses.
func benchRefreshAddrs(nAddrs int) []accounts.Address {
	addrs := make([]accounts.Address, nAddrs)
	for i := range addrs {
		var a common.Address
		a[0] = byte(i)
		a[1] = byte(i >> 8)
		a[19] = 0xAA
		addrs[i] = accounts.InternAddress(a)
	}
	return addrs
}

// benchSeedVersionMap writes balance/nonce/incarnation/codehash at tx 0 for
// every address, so a read at tx 1 resolves entirely in the versionMap
// (MapRead outcome) without touching the stateReader.
func benchSeedVersionMap(mvhm *VersionMap, addrs []accounts.Address) {
	v0 := Version{TxIndex: 0, Incarnation: 1}
	for i, addr := range addrs {
		mvhm.WriteBalance(addr, v0, *uint256.NewInt(uint64(100 + i)), true)
		mvhm.WriteNonce(addr, v0, uint64(i+1), true)
		mvhm.WriteIncarnation(addr, v0, uint64(1), true)
		var h common.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		mvhm.WriteCodeHash(addr, v0, accounts.InternCodeHash(h), true)
	}
}

// BenchmarkRefreshVersionedAccount isolates refreshVersionedAccount: it drives
// the 4 field reads (balance/nonce/incarnation/codehash) through the versionMap
// with no EVM overhead. "hit" seeds the versionMap so reads resolve there;
// "miss" leaves it empty so every read falls through to the caller default.
func BenchmarkRefreshVersionedAccount(b *testing.B) {
	const nAddrs = 256
	addrs := benchRefreshAddrs(nAddrs)

	run := func(b *testing.B, seed bool) {
		_, tx, domains := NewTestRwTx(b)
		mvhm := NewVersionMap(nil)
		if seed {
			benchSeedVersionMap(mvhm, addrs)
		}
		s := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
		s.txIndex = 1
		base := &accounts.Account{}

		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			//nolint:errcheck
			s.refreshVersionedAccount(addrs[i&(nAddrs-1)], base, StorageRead, UnknownVersion)
			i++
		}
	}

	b.Run("hit", func(b *testing.B) { run(b, true) })
	b.Run("miss", func(b *testing.B) { run(b, false) })
}

// BenchmarkRefreshVersionedAccountParallel exposes the VersionMap RWMutex
// contention: many goroutines each own their IntraBlockState but share one
// seeded versionMap, so every refresh takes the global RLock. The seeded map
// keeps all reads inside the versionMap (no shared stateReader access).
func BenchmarkRefreshVersionedAccountParallel(b *testing.B) {
	const nAddrs = 256
	addrs := benchRefreshAddrs(nAddrs)

	_, tx, domains := NewTestRwTx(b)
	mvhm := NewVersionMap(nil)
	benchSeedVersionMap(mvhm, addrs)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		s := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
		s.txIndex = 1
		base := &accounts.Account{}
		i := 0
		for pb.Next() {
			//nolint:errcheck
			s.refreshVersionedAccount(addrs[i&(nAddrs-1)], base, StorageRead, UnknownVersion)
			i++
		}
	})
}
