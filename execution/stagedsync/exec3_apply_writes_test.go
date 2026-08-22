// Copyright 2025 The Erigon Authors
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

package stagedsync

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestApplyStateWritesPreservesEarlierNonce(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))

	addr := accounts.InternAddress(common.HexToAddress("0xa1b2c3"))
	addrVal := addr.Value()
	seed := accounts.Account{Nonce: 5, Balance: *uint256.NewInt(1000)}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrVal[:], accounts.SerialiseV3(&seed), 0, nil))

	first := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(900)).
		nonce(addr, state.Version{}, 6).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 100, first, nil, &chain.Rules{}, nil))

	second := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(1100)).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 200, second, nil, &chain.Rules{}, nil))

	encoded, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var account accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&account, encoded))
	require.Equal(t, uint64(6), account.Nonce)
	require.Equal(t, uint64(1100), account.Balance.Uint64())
}

func TestApplyStateWritesPreservesNonceAcrossBalanceWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))

	addr := accounts.InternAddress(common.HexToAddress("0xdead"))
	addrVal := addr.Value()
	seed := accounts.Account{Nonce: 10, Balance: *uint256.NewInt(5000)}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrVal[:], accounts.SerialiseV3(&seed), 0, nil))

	first := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(4800)).
		nonce(addr, state.Version{}, 11).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 10, first, nil, &chain.Rules{}, nil))

	for i := range 4 {
		writes := newWS().
			bal(addr, state.Version{}, *uint256.NewInt(uint64(4900 + i*100))).
			build()
		require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, uint64(11+i), writes, nil, &chain.Rules{}, nil))
	}

	encoded, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)

	var account accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&account, encoded))
	require.Equal(t, uint64(11), account.Nonce)
	require.Equal(t, uint64(5200), account.Balance.Uint64())
}

func TestApplyStateWritesInitializesNewAccountCodeHash(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))

	addr := accounts.InternAddress(common.HexToAddress("0x1001"))
	addrVal := addr.Value()
	writes := newWS().
		bal(addr, state.Version{}, *uint256.NewInt(1000)).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 1, writes, nil, &chain.Rules{}, nil))

	encoded, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var account accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&account, encoded))
	require.Equal(t, uint64(1000), account.Balance.Uint64())
	require.Equal(t, uint64(0), account.Nonce)
	require.True(t, account.IsEmptyCodeHash())
	require.Equal(t, encoded, accounts.SerialiseV3(&account))
}

func TestApplyStateWritesRestoresStorageOrigin(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))

	contract := accounts.InternAddress(common.HexToAddress("0xc001"))
	contractVal := contract.Value()
	slot := accounts.InternKey(common.HexToHash("0xcb"))
	slotVal := slot.Value()
	composite := dbutils.GenerateStoragePlainKey(contractVal, slotVal)

	seed := accounts.Account{Nonce: 1}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, contractVal[:], accounts.SerialiseV3(&seed), 0, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, composite, uint256.NewInt(1).Bytes(), 0, nil))

	first := newWS().
		stor(contract, slot, state.Version{}, *uint256.NewInt(2)).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 10, first, nil, &chain.Rules{}, nil))

	second := newWS().
		stor(contract, slot, state.Version{}, *uint256.NewInt(1)).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 11, second, nil, &chain.Rules{}, nil))

	value, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(1).Bytes(), value)
}

func TestApplyStateWritesPreservesUnchangedStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))

	contract := accounts.InternAddress(common.HexToAddress("0xc002"))
	contractVal := contract.Value()
	guardSlot := accounts.InternKey(common.HexToHash("0xcb"))
	changedSlot := accounts.InternKey(common.HexToHash("0xfe"))
	guardKey := dbutils.GenerateStoragePlainKey(contractVal, guardSlot.Value())
	changedKey := dbutils.GenerateStoragePlainKey(contractVal, changedSlot.Value())

	seed := accounts.Account{Nonce: 1}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, contractVal[:], accounts.SerialiseV3(&seed), 0, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, guardKey, uint256.NewInt(1).Bytes(), 0, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, changedKey, uint256.NewInt(37).Bytes(), 0, nil))

	writes := newWS().
		stor(contract, guardSlot, state.Version{}, *uint256.NewInt(1)).
		stor(contract, changedSlot, state.Version{}, *uint256.NewInt(42)).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 10, writes, nil, &chain.Rules{}, nil))

	guardValue, _, err := domains.GetLatest(kv.StorageDomain, tx, guardKey)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(1).Bytes(), guardValue)

	changedValue, _, err := domains.GetLatest(kv.StorageDomain, tx, changedKey)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(42).Bytes(), changedValue)
}

func TestApplyStateWritesClearsCodeDomain(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, log.New()))

	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	addrVal := addr.Value()
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	codeHash := accounts.NewCode(code).Hash
	seed := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: codeHash}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrVal[:], accounts.SerialiseV3(&seed), 0, nil))
	require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, addrVal[:], code, 0, nil))

	writes := newWS().
		codeHash(addr, state.Version{}, accounts.EmptyCodeHash).
		code(addr, state.Version{}, accounts.EmptyCode).
		build()
	require.NoError(t, rs.ApplyStateWrites(context.Background(), tx, 1, 100, writes, nil, &chain.Rules{}, nil))

	got, _, err := domains.GetLatest(kv.CodeDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.Empty(t, got)
}
