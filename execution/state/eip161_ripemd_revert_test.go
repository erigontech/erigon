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

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type emptyAccountReader struct {
	NoopReader
	addr accounts.Address
	acc  *accounts.Account
}

func (r *emptyAccountReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	if address == r.addr {
		return r.acc, nil
	}
	return nil, nil
}

func (r *emptyAccountReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

// normalizeTouchThenRevert touches an empty account, reverts it, and reports whether EIP-161 still sweeps the account.
func normalizeTouchThenRevert(t *testing.T, addr accounts.Address) bool {
	t.Helper()

	empty := accounts.NewAccount()
	reader := &emptyAccountReader{addr: addr, acc: &empty}

	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	ibs.SetNoMaterialize(true)
	defer ibs.Close()
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.AddBalance(addr, uint256.Int{}, 0))
	ibs.RevertToSnapshot(snap, nil)

	writes := ibs.FinalizedWrites(&chain.Rules{IsSpuriousDragon: true})
	if writes == nil {
		return false
	}
	normalized, err := writes.Normalize(vm, 0, 0, reader, nil, true, false, false)
	require.NoError(t, err)
	if normalized == nil {
		return false
	}
	for a, w := range normalized.SelfDestructs() {
		if a == addr && w.Val {
			return true
		}
	}
	return false
}

// RIPEMD-160 is specially exempted: its touch outlives a revert, so EIP-161
// must still sweep it on the versioned path — unlike an ordinary account.
func TestEIP161RipemdTouchSurvivesRevertOnVersionedPath(t *testing.T) {
	t.Parallel()

	t.Run("ripemd is swept despite the revert", func(t *testing.T) {
		t.Parallel()
		require.True(t, normalizeTouchThenRevert(t, accounts.InternAddress(ripemd.Value())),
			"ripemd's touch outlives a revert, so EIP-161 must still remove it")
	})

	t.Run("ordinary account is not swept", func(t *testing.T) {
		t.Parallel()
		require.False(t, normalizeTouchThenRevert(t, toAddr([]byte("ordinary-empty"))),
			"a reverted touch must not remove an ordinary empty account")
	})
}
