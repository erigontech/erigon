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

package statetest

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func addr(h string) accounts.Address    { return accounts.InternAddress(common.HexToAddress(h)) }
func skey(h string) accounts.StorageKey { return accounts.InternKey(common.HexToHash(h)) }

func TestReader_Empty(t *testing.T) {
	t.Parallel()
	r := NewReader()
	acc, err := r.ReadAccountData(addr("0x01"))
	require.NoError(t, err)
	require.Nil(t, acc)

	size, err := r.ReadAccountCodeSize(addr("0x01"))
	require.NoError(t, err)
	require.Equal(t, 0, size)

	_, found, err := r.ReadAccountStorage(addr("0x01"), skey("0x02"))
	require.NoError(t, err)
	require.False(t, found)
}

func TestReader_Seeded(t *testing.T) {
	t.Parallel()
	a := addr("0x01")
	r := NewReader().
		WithBalance(a, uint256.NewInt(500)).
		WithNonce(a, 3).
		WithCode(a, []byte{0x60, 0x00}).
		WithStorage(a, skey("0x02"), uint256.NewInt(7))

	acc, err := r.ReadAccountData(a)
	require.NoError(t, err)
	require.NotNil(t, acc)
	require.Equal(t, uint64(500), acc.Balance.Uint64())
	require.Equal(t, uint64(3), acc.Nonce)

	code, err := r.ReadAccountCode(a)
	require.NoError(t, err)
	require.Equal(t, []byte{0x60, 0x00}, code)

	size, err := r.ReadAccountCodeSize(a)
	require.NoError(t, err)
	require.Equal(t, 2, size)

	has, err := r.HasStorage(a)
	require.NoError(t, err)
	require.True(t, has)

	val, found, err := r.ReadAccountStorage(a, skey("0x02"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), val.Uint64())
}

func TestReader_ReturnsCopy(t *testing.T) {
	t.Parallel()
	a := addr("0x01")
	r := NewReader().WithBalance(a, uint256.NewInt(100))

	got, err := r.ReadAccountData(a)
	require.NoError(t, err)
	got.Balance.SetUint64(999) // mutate the returned copy

	again, err := r.ReadAccountData(a)
	require.NoError(t, err)
	require.Equal(t, uint64(100), again.Balance.Uint64())
}

func TestReader_BuildsWritableState(t *testing.T) {
	t.Parallel()
	seeded := addr("0x01")
	fresh := addr("0x02")
	ibs := NewReader().WithBalance(seeded, uint256.NewInt(1000)).State()

	// Pre-seeded balance is visible.
	bal, err := ibs.GetBalance(seeded)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), bal.Uint64())

	// Fresh account does not exist until created/credited.
	exists, err := ibs.Exist(fresh)
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, ibs.AddBalance(fresh, *uint256.NewInt(42), tracing.BalanceChangeTransfer))
	bal, err = ibs.GetBalance(fresh)
	require.NoError(t, err)
	require.Equal(t, uint64(42), bal.Uint64())
}

func TestReader_Trace(t *testing.T) {
	t.Parallel()
	r := NewReader()
	r.SetTrace(true, "px")
	require.True(t, r.Trace())
	require.Equal(t, "px", r.TracePrefix())
}
