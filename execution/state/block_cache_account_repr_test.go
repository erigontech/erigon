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

	preA := reprAcc{nonce: 5, balance: 100, codeHash: codeHash, incarnation: 2}
	preB := reprAcc{nonce: 2, balance: 50}
	preD := reprAcc{nonce: 7, balance: 70}
	preE := reprAcc{nonce: 3, balance: 30}
	preF := reprAcc{nonce: 9, balance: 90}

	const preTxNum uint64 = 1
	for addr, pre := range map[accounts.Address]reprAcc{addrA: preA, addrB: preB, addrD: preD, addrE: preE, addrF: preF} {
		v := addr.Value()
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, v[:], pre.enc(), preTxNum, nil))
	}

	cache := NewBlockStateCache()
	committedB := preB.account()
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

	txA1 := reprAcc{nonce: 5, balance: 200, codeHash: codeHash, incarnation: 2}
	apply(10, newWriteSet(balanceWrite(addrA, 200, 0)), nil)
	requireCurrent(addrA, &txA1)

	txA2 := reprAcc{nonce: 6, balance: 200, codeHash: codeHash, incarnation: 2}
	apply(11, newWriteSet(nonceWrite(addrA, 6, 0)), nil)
	requireCurrent(addrA, &txA2)

	committedView, err := history.ReadAccountData(addrB)
	require.NoError(t, err)
	expB := preB.account()
	require.Equal(t, &expB, committedView)

	txB := reprAcc{nonce: 2, balance: 60}
	apply(12, newWriteSet(balanceWrite(addrB, 60, 0)), nil)
	requireCurrent(addrB, &txB)

	txC := reprAcc{balance: 1}
	apply(13, newWriteSet(balanceWrite(addrC, 1, 0), codeHashWrite(addrC, 0)), nil)
	requireCurrent(addrC, &txC)

	apply(14, newWriteSet(selfDestructWrite(addrD, true)), nil)
	requireCurrent(addrD, nil)

	txD := reprAcc{nonce: 7, balance: 5}
	apply(15, newWriteSet(balanceWrite(addrD, 5, 0)), nil)
	requireCurrent(addrD, &txD)

	apply(16, newWriteSet(selfDestructWrite(addrE, true)), nil)
	requireCurrent(addrE, nil)

	txE := reprAcc{balance: 4}
	apply(17, nil, map[accounts.Address]uint256.Int{addrE: *uint256.NewInt(4)})
	requireCurrent(addrE, &txE)

	txA3 := reprAcc{nonce: 6, balance: 201, codeHash: codeHash, incarnation: 2}
	apply(18, nil, map[accounts.Address]uint256.Int{addrA: *uint256.NewInt(1)})
	requireCurrent(addrA, &txA3)

	txF := reprAcc{nonce: 9, balance: 100}
	apply(19, nil, map[accounts.Address]uint256.Int{addrF: *uint256.NewInt(10)})
	requireCurrent(addrF, &txF)

	require.NoError(t, cache.Flush(domains, tx))

	asOf := func(addr accounts.Address, txNum uint64) []byte {
		t.Helper()
		v := addr.Value()
		enc, _, err := domains.GetAsOf(kv.AccountsDomain, v[:], txNum)
		require.NoError(t, err)
		return enc
	}
	latest := func(addr accounts.Address) []byte {
		t.Helper()
		v := addr.Value()
		enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, v[:])
		require.NoError(t, err)
		return enc
	}

	require.Equal(t, txA1.enc(), asOf(addrA, 11))
	require.Equal(t, txA2.enc(), asOf(addrA, 12))
	require.Equal(t, txA3.enc(), latest(addrA))
	require.Equal(t, txB.enc(), latest(addrB))
	require.Equal(t, txC.enc(), latest(addrC))
	require.Empty(t, asOf(addrD, 15))
	require.Equal(t, txD.enc(), latest(addrD))
	require.Empty(t, asOf(addrE, 17))
	require.Equal(t, txE.enc(), latest(addrE))
	require.Equal(t, txF.enc(), latest(addrF))
}

func TestBlockStateCacheKeepsEmptyAccountsBeforeEIP161(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	written := accounts.InternAddress(common.HexToAddress("0xe1"))
	increased := accounts.InternAddress(common.HexToAddress("0xe2"))
	committed := accounts.InternAddress(common.HexToAddress("0xe3"))
	emptied := accounts.InternAddress(common.HexToAddress("0xe4"))

	funded := accounts.NewAccount()
	funded.Balance.SetUint64(5)
	emptiedKey := emptied.Value()
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, emptiedKey[:], accounts.SerialiseV3(&funded), 1, nil))

	cache := NewBlockStateCache()
	emptyAcc := accounts.NewAccount()
	cache.PutCommittedAccount(committed, &emptyAcc)

	rules := &chain.Rules{}
	require.False(t, rules.IsEIP161Enabled())
	ws := newWriteSet(
		balanceWrite(written, 0, 0),
		nonceWrite(written, 0, 0),
		incarnationWrite(written, 0),
		&VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: written, Path: CodeHashPath}, Val: accounts.EmptyCodeHash},
		balanceWrite(emptied, 0, 0),
	)
	require.NoError(t, ws.Apply(domains, tx, 1, 10, map[accounts.Address]uint256.Int{increased: {}}, rules, cache, false))
	require.NoError(t, newWriteSet(nonceWrite(emptied, 1, 0)).Apply(domains, tx, 1, 11, nil, rules, cache, false))

	current := NewCurrentCachedReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}), cache)
	history := NewHistoryReaderV3WithBlockCache(tx, domains, cache, 100)
	for _, addr := range []accounts.Address{written, increased, committed} {
		got, err := current.ReadAccountData(addr)
		require.NoError(t, err)
		require.NotNil(t, got, "current view lost empty account %x", addr.Value())
		require.True(t, got.Empty())
		hist, err := history.ReadAccountData(addr)
		require.NoError(t, err)
		require.NotNil(t, hist, "history view lost empty account %x", addr.Value())
		has, err := history.HasAccount(addr)
		require.NoError(t, err)
		require.True(t, has, "history view reports empty account %x as absent", addr.Value())
	}

	got, err := current.ReadAccountData(emptied)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Balance.IsZero(), "base for a write after an empty account must be that empty account, not the pre-block domain value")
	require.Equal(t, uint64(1), got.Nonce)

	require.NoError(t, cache.Flush(domains, tx))
	for _, addr := range []accounts.Address{written, increased} {
		v := addr.Value()
		enc, _, err := domains.GetLatest(kv.AccountsDomain, tx, v[:])
		require.NoError(t, err)
		require.Equal(t, accounts.SerialiseV3(&emptyAcc), enc, "flush must write empty account %x, not delete it", v)
	}
}
