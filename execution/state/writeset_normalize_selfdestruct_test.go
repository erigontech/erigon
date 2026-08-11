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
	"github.com/erigontech/erigon/execution/types/accounts"
)

// preBlockContractReader answers with a live contract account, as the domain
// still holds it for an address destroyed later in the same block.
type preBlockContractReader struct {
	minimalStateReader
	acc accounts.Account
}

func (r *preBlockContractReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	a := r.acc
	return &a, nil
}

// A write set that credits an address self-destructed by an earlier tx of the
// same block must not recover that address's pre-destruct fields. The block
// finalize does exactly this when the coinbase is a contract a tx destroyed:
// it writes only a balance, so hasCreateContract is false and the version map
// holds no nonce or code-hash cell. Without this rule the completion loop falls
// through to the state reader, which serves the pre-block record and resurrects
// the destroyed contract's code hash into the commitment.
func TestNormalize_SelfDestructedEarlierKeepsFieldsZero(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x8888f1f195afa192cfee860698584c030f4c9db1"))
	const destructTx, finalizeTx = 0, 1
	ver := Version{TxIndex: finalizeTx}

	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: destructTx}, true, true)

	// The finalize write set credits the block reward and nothing else.
	ws := &WriteSet{}
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver},
		Val:         *uint256.NewInt(5_000_000_000),
	})

	liveCodeHash := accounts.InternCodeHash(common.HexToHash("0x4618e9572bed7958746ccb36021d590b2b0c3b416b771a34c9a463c7b1f8ad40"))
	reader := &preBlockContractReader{
		acc: accounts.Account{Nonce: 3, Balance: *uint256.NewInt(1000), CodeHash: liveCodeHash},
	}

	out, err := ws.Normalize(vm, finalizeTx, 0, reader, nil, false /*emptyRemoval*/, false /*isAura*/, false /*eip8246*/)
	require.NoError(t, err)

	gotCodeHash, ok := out.GetCodeHash(addr)
	require.True(t, ok, "code hash must be emitted so the commitment sees a full account")
	require.Equal(t, accounts.EmptyCodeHash, gotCodeHash.Val,
		"a destroyed account must not recover its pre-destruct code hash from the state reader")

	gotNonce, ok := out.GetNonce(addr)
	require.True(t, ok)
	require.Zero(t, gotNonce.Val, "a destroyed account must not recover its pre-destruct nonce")

	gotBalance, ok := out.GetBalance(addr)
	require.True(t, ok, "the credit itself must survive")
	require.Equal(t, uint256.NewInt(5_000_000_000).String(), gotBalance.Val.String())
}
