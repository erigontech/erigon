// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type fixedTemporalTx struct {
	kv.TemporalTx
	val []byte
}

func (g fixedTemporalTx) GetLatest(name kv.Domain, k []byte, _ ...kv.GetLatestOption) ([]byte, kv.Step, error) {
	return g.val, 0, nil
}
func (g fixedTemporalTx) GetLatestValSize(name kv.Domain, k []byte) (int, bool, error) {
	return len(g.val), len(g.val) > 0, nil
}
func (g fixedTemporalTx) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}
func (g fixedTemporalTx) StepsInFiles(entitySet ...kv.Domain) kv.Step { return 0 }

type histMockTx struct {
	kv.TemporalTx
	val []byte
}

func (m histMockTx) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	return m.val, true, nil
}

func TestStateReader_ReadMethods_Allocs(t *testing.T) {
	var acc accounts.Account
	acc.Nonce = 1
	accEnc := accounts.SerialiseV3(&acc) // valid encoding so ReadAccountData hits the deserialize path

	r := NewReaderV3(execctx.NewTemporalTxStateGetter(fixedTemporalTx{val: accEnc}))
	addr := accounts.InternAddress(common.Address{0x11})
	key := accounts.InternKey(common.Hash{0x22})
	hr := NewHistoryReaderV3(histMockTx{val: accEnc}, 0)

	cache := NewBlockStateCache()
	cache.PutCommittedStorage(addr, key, make([]byte, 32))
	cache.PutCommittedAccount(addr, &acc)
	cr := NewCachedReaderV3(execctx.NewTemporalTxStateGetter(fixedTemporalTx{val: make([]byte, 32)}), cache)

	for _, tc := range []struct {
		name string
		want float64
		fn   func()
	}{
		{"ReaderV3.ReadAccountStorage", 0, func() { _, _, _ = r.ReadAccountStorage(addr, key) }},
		{"ReaderV3.ReadAccountData", 1, func() { _, _ = r.ReadAccountData(addr) }}, // 1: returns *accounts.Account
		{"ReaderV3.HasStorage", 0, func() { _, _ = r.HasStorage(addr) }},
		{"ReaderV3.ReadAccountCode", 0, func() { _, _ = r.ReadAccountCode(addr) }},
		{"ReaderV3.ReadAccountCodeSize", 0, func() { _, _ = r.ReadAccountCodeSize(addr) }},
		{"ReaderV3.ReadAccountDataForDebug", 1, func() { _, _ = r.ReadAccountDataForDebug(addr) }}, // 1: returns *accounts.Account
		{"ReaderV3.ReadAccountIncarnation", 0, func() { _, _ = r.ReadAccountIncarnation(addr) }},

		{"HistoryReaderV3.ReadAccountStorage", 0, func() { _, _, _ = hr.ReadAccountStorage(addr, key) }},
		{"HistoryReaderV3.ReadAccountCode", 0, func() { _, _ = hr.ReadAccountCode(addr) }},
		{"HistoryReaderV3.ReadAccountCodeSize", 0, func() { _, _ = hr.ReadAccountCodeSize(addr) }},
		{"HistoryReaderV3.ReadAccountData", 1, func() { _, _ = hr.ReadAccountData(addr) }},                 // 1: returns *accounts.Account
		{"HistoryReaderV3.ReadAccountDataForDebug", 1, func() { _, _ = hr.ReadAccountDataForDebug(addr) }}, // 1: returns *accounts.Account

		{"CachedReaderV3.ReadAccountStorage (cache hit)", 0, func() { _, _, _ = cr.ReadAccountStorage(addr, key) }},
		{"CachedReaderV3.ReadAccountData (cache hit)", 1, func() { _, _ = cr.ReadAccountData(addr) }}, // 1: returns *accounts.Account
		{"CachedReaderV3.ReadAccountCode", 0, func() { _, _ = cr.ReadAccountCode(addr) }},
		{"CachedReaderV3.ReadAccountCodeSize", 0, func() { _, _ = cr.ReadAccountCodeSize(addr) }},
		{"CachedReaderV3.HasStorage", 0, func() { _, _ = cr.HasStorage(addr) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, tc.fn)
			require.Equal(t, tc.want, allocs, "%s: alloc count changed", tc.name)
		})
	}
}

func cacheReadTestAccount() *accounts.Account {
	acc := accounts.NewAccount()
	acc.Nonce = 42
	acc.Balance = *uint256.NewInt(1e18)
	acc.Incarnation = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x00}))
	return &acc
}

func TestCachedReaderV3_CurrentReadsCommittedWhenUnwritten(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	want := cacheReadTestAccount()

	cache := NewBlockStateCache()
	cache.PutCommittedAccount(addr, want)
	got, err := NewCurrentCachedReaderV3(nil, cache).ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, want.Nonce, got.Nonce)
	require.Equal(t, want.Balance, got.Balance)
	require.Equal(t, want.Incarnation, got.Incarnation)
	require.Equal(t, want.CodeHash, got.CodeHash)
	require.NotSame(t, want, got, "the caller must not be able to mutate the cached account")
}

// A write this block shadows the committed view, and a nil write means the
// account was destroyed — neither may fall through to the committed entry.
func TestCachedReaderV3_CurrentPrefersBlockWrite(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	cache := NewBlockStateCache()
	cache.PutCommittedAccount(addr, cacheReadTestAccount())

	written := cacheReadTestAccount()
	written.Nonce = 43
	cache.WriteAccount(addr, accounts.SerialiseV3(written), 1)
	got, err := NewCurrentCachedReaderV3(nil, cache).ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(43), got.Nonce)

	cache.WriteAccount(addr, nil, 2)
	got, err = NewCurrentCachedReaderV3(nil, cache).ReadAccountData(addr)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestCachedReaderV3_CurrentReturnsNilForCommittedAbsence(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	cache := NewBlockStateCache()
	cache.PutCommittedAccount(addr, nil)
	got, err := NewCurrentCachedReaderV3(nil, cache).ReadAccountData(addr)
	require.NoError(t, err)
	require.Nil(t, got)
}

// BenchmarkCachedReaderAccountRead prices one apply-loop account read that hits
// the block state cache, on each of its two paths.
func BenchmarkCachedReaderAccountRead(b *testing.B) {
	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	acc := cacheReadTestAccount()

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

	b.Run("written", func(b *testing.B) {
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
}
