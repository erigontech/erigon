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

package misc

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state/statetest"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestTransfer(t *testing.T) {
	t.Parallel()
	sender := accounts.InternAddress(common.HexToAddress("0x01"))
	recipient := accounts.InternAddress(common.HexToAddress("0x02"))

	ibs := statetest.NewReader().WithBalance(sender, uint256.NewInt(100)).State()
	require.NoError(t, Transfer(ibs, sender, recipient, *uint256.NewInt(30), false, &chain.Rules{}))

	sBal, err := ibs.GetBalance(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(70), sBal.Uint64())
	rBal, err := ibs.GetBalance(recipient)
	require.NoError(t, err)
	require.Equal(t, uint64(30), rBal.Uint64())
}

func TestTransfer_Bailout(t *testing.T) {
	t.Parallel()
	sender := accounts.InternAddress(common.HexToAddress("0x01"))
	recipient := accounts.InternAddress(common.HexToAddress("0x02"))

	// bailout=true credits the recipient without debiting the sender.
	ibs := statetest.NewReader().WithBalance(sender, uint256.NewInt(100)).State()
	require.NoError(t, Transfer(ibs, sender, recipient, *uint256.NewInt(30), true, &chain.Rules{}))

	sBal, _ := ibs.GetBalance(sender)
	require.Equal(t, uint64(100), sBal.Uint64())
	rBal, _ := ibs.GetBalance(recipient)
	require.Equal(t, uint64(30), rBal.Uint64())
}

func TestApplyDAOHardFork(t *testing.T) {
	t.Parallel()
	drained := DAODrainList()[0]

	ibs := statetest.NewReader().WithBalance(drained, uint256.NewInt(1000)).State()
	require.NoError(t, ApplyDAOHardFork(ibs))

	dBal, err := ibs.GetBalance(drained)
	require.NoError(t, err)
	require.Equal(t, uint64(0), dBal.Uint64())

	refund, err := ibs.GetBalance(DAORefundContract)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), refund.Uint64())
}

func TestStoreBlockHashesEip2935(t *testing.T) {
	t.Parallel()
	header := &types.Header{Number: *uint256.NewInt(10), ParentHash: common.HexToHash("0xabc")}

	// No code deployed at the history-storage address -> no-op.
	noCode := statetest.NewReader().State()
	require.NoError(t, StoreBlockHashesEip2935(header, noCode))

	// With code, the parent hash is written at slot (number-1) % window.
	ibs := statetest.NewReader().WithCode(params.HistoryStorageAddress, []byte{0x01}).State()
	require.NoError(t, StoreBlockHashesEip2935(header, ibs))

	slot := accounts.InternKey(common.BytesToHash(uint256.NewInt((10 - 1) % params.BlockHashHistoryServeWindow).Bytes()))
	got, err := ibs.GetState(params.HistoryStorageAddress, slot)
	require.NoError(t, err)
	want := uint256.NewInt(0).SetBytes32(header.ParentHash.Bytes())
	require.Equal(t, want.Hex(), got.Hex())
}

func TestDequeueWithdrawalRequests7002(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x00000961ef480eb55e80d19ad83579a64c007002"))

	okCall := func(accounts.Address, []byte) ([]byte, error) { return []byte{0xaa}, nil }

	// Empty code -> error.
	_, err := DequeueWithdrawalRequests7002(okCall, statetest.NewReader().State(), addr)
	require.Error(t, err)

	withCode := statetest.NewReader().WithCode(addr, []byte{0x01}).State()
	req, err := DequeueWithdrawalRequests7002(okCall, withCode, addr)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, types.WithdrawalRequestType, req.Type)

	// Syscall error is wrapped.
	failCall := func(accounts.Address, []byte) ([]byte, error) { return nil, errors.New("boom") }
	_, err = DequeueWithdrawalRequests7002(failCall, withCode, addr)
	require.Error(t, err)

	// Nil result -> nil request.
	nilCall := func(accounts.Address, []byte) ([]byte, error) { return nil, nil }
	req, err = DequeueWithdrawalRequests7002(nilCall, withCode, addr)
	require.NoError(t, err)
	require.Nil(t, req)
}

func TestDequeueConsolidationRequests7251(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x0000bbddc7ce488642fb579f8b00f3a590007251"))
	okCall := rules.SystemCall(func(accounts.Address, []byte) ([]byte, error) { return []byte{0xbb}, nil })

	_, err := DequeueConsolidationRequests7251(okCall, statetest.NewReader().State(), addr)
	require.Error(t, err)

	withCode := statetest.NewReader().WithCode(addr, []byte{0x01}).State()
	req, err := DequeueConsolidationRequests7251(okCall, withCode, addr)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, types.ConsolidationRequestType, req.Type)
}
