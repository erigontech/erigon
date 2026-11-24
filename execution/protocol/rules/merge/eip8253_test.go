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

package merge

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type nonceBumpChainReader struct {
	readerMock
	parent *types.Header
}

func (r nonceBumpChainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if r.parent != nil && r.parent.Hash() == hash && r.parent.Number.Uint64() == number {
		return r.parent
	}
	return nil
}

func TestEIP8253BeforeSystemCalls(t *testing.T) {
	t.Run("genesis parent", func(t *testing.T) { testEIP8253BeforeSystemCalls(t, 0) })
	t.Run("later parent", func(t *testing.T) { testEIP8253BeforeSystemCalls(t, 1) })
}

func testEIP8253BeforeSystemCalls(t *testing.T, parentNumber uint64) {
	t.Helper()
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(100)
	config.ZeroNonceStorageAccounts = []common.Address{common.HexToAddress("0x8253")}
	parent := &types.Header{Number: *uint256.NewInt(parentNumber), Time: 99}
	header := &types.Header{
		Number: *uint256.NewInt(parentNumber + 1), ParentHash: parent.Hash(), Time: 100,
		ParentBeaconBlockRoot: new(common.Hash),
	}
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	target := accounts.InternAddress(common.HexToAddress("0x8253"))
	key := accounts.InternKey(common.Hash{1})
	require.NoError(t, ibs.SetBalance(target, uint256.Int{100}, tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetState(target, key, uint256.Int{42}))
	called := false
	syscall := func(_ accounts.Address, _ []byte, ibs *state.IntraBlockState, _ *types.Header, _ bool) ([]byte, error) {
		called = true
		nonce, err := ibs.GetNonce(target)
		require.NoError(t, err)
		require.EqualValues(t, 1, nonce, "the nonce bump precedes system calls")
		return nil, nil
	}
	reader := nonceBumpChainReader{readerMock{config: config}, parent}
	require.NoError(t, New(nil).Initialize(config, reader, header, ibs, syscall, log.New(), nil))
	require.True(t, called)
	balance, err := ibs.GetBalance(target)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{100}, balance)
	value, err := ibs.GetState(target, key)
	require.NoError(t, err)
	require.Equal(t, uint256.Int{42}, value)
	code, err := ibs.GetCode(target)
	require.NoError(t, err)
	require.Empty(t, code)
	require.Empty(t, ibs.Logs())
}
