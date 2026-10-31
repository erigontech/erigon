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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestParentStateServesOnlyPreBlockReads(t *testing.T) {
	stored, written, absent := accounts.InternAddress(common.Address{1}), accounts.InternAddress(common.Address{2}), accounts.InternAddress(common.Address{3})
	slotA, slotB := accounts.InternKey(common.Hash{1}), accounts.InternKey(common.Hash{2})
	pre := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(10), CodeHash: accounts.EmptyCodeHash}
	mid := accounts.Account{Nonce: 2, Balance: *uint256.NewInt(20), CodeHash: accounts.EmptyCodeHash}
	fromStorage := state.ReadHeader{Source: state.StorageRead}
	fromMap := state.ReadHeader{Source: state.MapRead}

	io := state.NewVersionedIO(2)
	var first state.ReadSet
	first.SetAddress(stored, state.VersionedRead[state.AccountView]{ReadHeader: fromStorage, Val: state.NewAccountView(&pre)})
	first.SetAddress(absent, state.VersionedRead[state.AccountView]{ReadHeader: fromStorage, Val: state.NewAccountView(nil)})
	first.SetStorage(stored, slotA, state.VersionedRead[uint256.Int]{ReadHeader: fromStorage, Val: *uint256.NewInt(7)})
	io.RecordReads(state.Version{TxIndex: 0}, first)
	var second state.ReadSet
	second.SetAddress(stored, state.VersionedRead[state.AccountView]{ReadHeader: fromMap, Val: state.NewAccountView(&mid)})
	second.SetAddress(written, state.VersionedRead[state.AccountView]{ReadHeader: fromMap, Val: state.NewAccountView(&mid)})
	second.SetStorage(stored, slotB, state.VersionedRead[uint256.Int]{ReadHeader: fromMap, Val: *uint256.NewInt(9)})
	io.RecordReads(state.Version{TxIndex: 1}, second)

	p := parentStateFromReads(io)
	acc, ok := p.GetCommittedAccount(stored)
	require.True(t, ok)
	require.Equal(t, pre, *acc, "a later in-block read never overrides the pre-block value")
	acc, ok = p.GetCommittedAccount(absent)
	require.True(t, ok)
	require.Nil(t, acc, "a pre-block read of a missing account is a known absence")
	_, ok = p.GetCommittedAccount(written)
	require.False(t, ok, "an account read only from an earlier tx's write is not parent state")
	val, ok := p.GetCommittedStorage(stored, slotA)
	require.True(t, ok)
	require.Equal(t, []byte{7}, val)
	_, ok = p.GetCommittedStorage(stored, slotB)
	require.False(t, ok, "a slot read only from an earlier tx's write is not parent state")
}
