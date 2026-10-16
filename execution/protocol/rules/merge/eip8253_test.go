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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type eip8253ChainReader struct {
	readerMock
	parent *types.Header
}

func (r eip8253ChainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if r.parent != nil && r.parent.Hash() == hash && r.parent.Number.Uint64() == number {
		return r.parent
	}
	return nil
}

func TestEIP8253NonceBumpBeforeSystemCalls(t *testing.T) {
	t.Parallel()

	config := chainspec.Mainnet.Config.Copy()
	config.AmsterdamTime = common.NewUint64(*config.OsakaTime + 100)
	parent := &types.Header{Number: *uint256.NewInt(1), Time: *config.AmsterdamTime - 1}
	header := &types.Header{
		Number:                *uint256.NewInt(2),
		ParentHash:            parent.Hash(),
		Time:                  *config.AmsterdamTime,
		ParentBeaconBlockRoot: new(common.Hash),
	}
	reader := eip8253ChainReader{readerMock{config: config}, parent}
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	addr := accounts.InternAddress(common.HexToAddress("0xf468bcbc4a0bfdb06336e773382c5202e674db71"))
	key := accounts.InternKey(common.Hash{})
	balance, value := *uint256.NewInt(100), *uint256.NewInt(42)
	require.NoError(t, ibs.SetBalance(addr, balance, tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetState(addr, key, value))

	called := false
	syscall := func(_ accounts.Address, _ []byte, ibs *state.IntraBlockState, _ *types.Header, _ bool) ([]byte, error) {
		called = true
		nonce, err := ibs.GetNonce(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce)
		return nil, nil
	}
	require.NoError(t, New(nil).Initialize(config, reader, header, ibs, syscall, log.New(), nil))
	require.True(t, called)
	gotBalance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, balance, gotBalance)
	gotValue, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, value, gotValue)
	code, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Empty(t, code)
	require.Empty(t, ibs.Logs())
}

func TestEIP8253OnlyAtForkBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		forkTime   *uint64
		parentTime uint64
		blockTime  uint64
		wantNonce  uint64
	}{
		{"unscheduled", nil, 90, 100, 0},
		{"before fork", common.NewUint64(100), 80, 90, 0},
		{"fork block", common.NewUint64(100), 90, 100, 1},
		{"missed fork slot", common.NewUint64(100), 90, 110, 1},
		{"after fork", common.NewUint64(100), 100, 110, 0},
		{"after missed fork slot", common.NewUint64(100), 110, 120, 0},
		{"active at genesis", common.NewUint64(0), 0, 10, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr := common.HexToAddress("0x8253")
			config := &chain.Config{AmsterdamTime: tc.forkTime, EIP8253Accounts: []common.Address{addr}}
			parent := &types.Header{Time: tc.parentTime}
			header := &types.Header{Number: *uint256.NewInt(1), ParentHash: parent.Hash(), Time: tc.blockTime}
			reader := eip8253ChainReader{readerMock{config: config}, parent}
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, New(nil).Initialize(config, reader, header, ibs, nil, log.New(), nil))
			nonce, err := ibs.GetNonce(accounts.InternAddress(addr))
			require.NoError(t, err)
			require.Equal(t, tc.wantNonce, nonce)
		})
	}
}

func TestEIP8253ChangedAccounts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		nonce uint64
		code  []byte
	}{
		{"nonce changed", 2, nil},
		{"code changed", 0, []byte{0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr := accounts.InternAddress(common.HexToAddress("0x8253"))
			config := &chain.Config{AmsterdamTime: common.NewUint64(100), EIP8253Accounts: []common.Address{addr.Value()}}
			parent := &types.Header{Time: 90}
			header := &types.Header{Number: *uint256.NewInt(1), ParentHash: parent.Hash(), Time: 100}
			reader := eip8253ChainReader{readerMock{config: config}, parent}
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetNonce(addr, tc.nonce, tracing.NonceChangeUnspecified))
			require.NoError(t, ibs.SetCode(addr, tc.code, tracing.CodeChangeUnspecified))
			require.NoError(t, New(nil).Initialize(config, reader, header, ibs, nil, log.New(), nil))
			nonce, err := ibs.GetNonce(addr)
			require.NoError(t, err)
			require.Equal(t, tc.nonce, nonce)
			code, err := ibs.GetCode(addr)
			require.NoError(t, err)
			require.Equal(t, tc.code, code)
		})
	}
}

func TestEIP8253RequiresParent(t *testing.T) {
	t.Parallel()
	config := &chain.Config{AmsterdamTime: common.NewUint64(100), EIP8253Accounts: []common.Address{{1}}}
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	header := &types.Header{Number: *uint256.NewInt(1), Time: 100}
	err := New(nil).Initialize(config, readerMock{config: config}, header, ibs, nil, log.New(), nil)
	require.ErrorIs(t, err, rules.ErrUnknownAncestor)
}

func TestEIP8253EmptyAccountList(t *testing.T) {
	t.Parallel()
	config := &chain.Config{AmsterdamTime: common.NewUint64(100)}
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	header := &types.Header{Number: *uint256.NewInt(1), Time: 100}
	require.NoError(t, New(nil).Initialize(config, readerMock{config: config}, header, ibs, nil, log.New(), nil))
	addr := accounts.InternAddress(common.HexToAddress("0xf468bcbc4a0bfdb06336e773382c5202e674db71"))
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestEIP8253StateError(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8253"))
	config := &chain.Config{AmsterdamTime: common.NewUint64(100), EIP8253Accounts: []common.Address{addr.Value()}}
	parent := &types.Header{Time: 90}
	header := &types.Header{Number: *uint256.NewInt(1), ParentHash: parent.Hash(), Time: 100}
	reader := eip8253ChainReader{readerMock{config: config}, parent}
	wantErr := errors.New("account read failed")
	ibs := state.New(withdrawalErrReader{StateReader: state.NewNoopReader(), fail: addr, err: wantErr})
	defer ibs.Close()
	err := New(nil).Initialize(config, reader, header, ibs, nil, log.New(), nil)
	require.ErrorIs(t, err, wantErr)
}
