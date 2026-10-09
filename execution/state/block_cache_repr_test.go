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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type reprAcc struct {
	nonce       uint64
	balance     uint64
	codeHash    accounts.CodeHash
	incarnation uint64
}

func (r reprAcc) account() accounts.Account {
	a := accounts.NewAccount()
	a.Nonce = r.nonce
	a.Balance.SetUint64(r.balance)
	if !r.codeHash.IsEmpty() {
		a.CodeHash = r.codeHash
	}
	a.Incarnation = r.incarnation
	return a
}

func (r reprAcc) enc() []byte {
	a := r.account()
	return accounts.SerialiseV3(&a)
}

func TestBlockStateCacheAccountRepresentation(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	codeHash := accounts.InternCodeHash(common.HexToHash("0xc0de"))
	addrA := accounts.InternAddress(common.HexToAddress("0xa1"))
	addrB := accounts.InternAddress(common.HexToAddress("0xb2"))
	addrC := accounts.InternAddress(common.HexToAddress("0xc3"))
	addrD := accounts.InternAddress(common.HexToAddress("0xd4"))
	addrE := accounts.InternAddress(common.HexToAddress("0xe5"))
	addrF := accounts.InternAddress(common.HexToAddress("0xf6"))
	empty := accounts.InternAddress(common.HexToAddress("0xe0"))

	for addr, pre := range map[accounts.Address]reprAcc{
		addrA: {nonce: 5, balance: 100, codeHash: codeHash, incarnation: 2},
		addrD: {nonce: 7, balance: 70},
		addrE: {nonce: 3, balance: 30},
		addrF: {nonce: 9, balance: 90},
	} {
		v := addr.Value()
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, v[:], pre.enc(), 1, nil))
	}

	cache := NewBlockStateCache()
	committedB := reprAcc{nonce: 2, balance: 50}.account()
	committedB.PrevIncarnation = 9
	committedB.Root = common.HexToHash("0x1234")
	cache.PutCommittedAccount(addrB, &committedB)

	apply := func(txNum uint64, ws *WriteSet, increases map[accounts.Address]uint256.Int) {
		t.Helper()
		require.NoError(t, ws.Apply(domains, tx, 1, txNum, increases, &chain.Rules{}, cache, false))
	}
	current := NewCurrentCachedReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}), cache)
	history := NewHistoryReaderV3WithBlockCache(tx, domains, cache, 100)

	requireCurrent := func(addr accounts.Address, want *reprAcc) {
		t.Helper()
		got, err := current.ReadAccountData(addr)
		require.NoError(t, err)
		hist, err := history.ReadAccountData(addr)
		require.NoError(t, err)
		has, err := history.HasAccount(addr)
		require.NoError(t, err)
		if want == nil {
			require.Nil(t, got)
			require.Nil(t, hist)
			require.False(t, has)
			return
		}
		exp := want.account()
		require.Equal(t, &exp, got)
		require.Equal(t, &exp, hist)
		require.True(t, has)
	}

	apply(10, newWriteSet(balanceWrite(addrA, 200, 0)), nil)
	requireCurrent(addrA, &reprAcc{nonce: 5, balance: 200, codeHash: codeHash, incarnation: 2})

	apply(11, newWriteSet(nonceWrite(addrA, 6, 0)), nil)
	requireCurrent(addrA, &reprAcc{nonce: 6, balance: 200, codeHash: codeHash, incarnation: 2})

	committedView, err := history.ReadAccountData(addrB)
	require.NoError(t, err)
	require.Equal(t, reprAcc{nonce: 2, balance: 50}.account(), *committedView)

	apply(12, newWriteSet(balanceWrite(addrC, 1, 0), codeHashWrite(addrC, 0)), nil)
	requireCurrent(addrC, &reprAcc{balance: 1})

	apply(13, newWriteSet(selfDestructWrite(addrD, true)), nil)
	requireCurrent(addrD, nil)

	apply(14, newWriteSet(balanceWrite(addrD, 5, 0)), nil)
	requireCurrent(addrD, &reprAcc{nonce: 7, balance: 5})

	apply(15, newWriteSet(selfDestructWrite(addrE, true)), nil)
	apply(16, nil, map[accounts.Address]uint256.Int{addrE: *uint256.NewInt(4)})
	requireCurrent(addrE, &reprAcc{balance: 4})

	apply(17, nil, map[accounts.Address]uint256.Int{addrF: *uint256.NewInt(10)})
	requireCurrent(addrF, &reprAcc{nonce: 9, balance: 100})

	apply(18, newWriteSet(balanceWrite(empty, 0, 0)), nil)
	requireCurrent(empty, &reprAcc{})
}

func TestBlockStateCacheStorageRepresentation(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	token := accounts.InternAddress(common.HexToAddress("0x70ce"))
	present := accounts.InternKey(common.HexToHash("0x01"))
	absent := accounts.InternKey(common.HexToHash("0x02"))
	updated := accounts.InternKey(common.HexToHash("0x03"))
	cleared := accounts.InternKey(common.HexToHash("0x04"))
	for _, k := range []accounts.StorageKey{present, cleared} {
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, []byte(storageCacheKey(token, k)), []byte{0x11}, 1, nil))
	}

	cache := NewBlockStateCache()
	getter := domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	worker := NewCachedReaderV3(getter, cache)
	current := NewCurrentCachedReaderV3(getter, cache)
	history := NewHistoryReaderV3WithBlockCache(tx, domains, cache, 100)

	type slot struct {
		val uint64
		ok  bool
	}
	read := func(r StateReader, k accounts.StorageKey) slot {
		t.Helper()
		v, ok, err := r.ReadAccountStorage(token, k)
		require.NoError(t, err)
		return slot{v.Uint64(), ok}
	}

	for range 2 {
		require.Equal(t, slot{0x11, true}, read(worker, present))
		require.Equal(t, slot{0, false}, read(worker, absent))
	}

	ws := newWriteSet(storageWrite(token, updated, 0x10), storageWrite(token, cleared, 0))
	require.NoError(t, ws.Apply(domains, tx, 1, 10, nil, &chain.Rules{}, cache, false))
	for _, r := range []StateReader{current, history} {
		require.Equal(t, slot{0x10, true}, read(r, updated))
		require.Equal(t, slot{0, false}, read(r, cleared))
	}
}
