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
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// balFoldRoot folds a two-account BAL block to its block-end root, with or without a step checkpoint at tx index 4 first,
// and starting the block-end fold after tx index after. With destroy, account x is emptied at tx 1 (an EIP-161 removal)
// and revived later, with a slot written after the revival.
func balFoldRoot(t *testing.T, checkpoint bool, after int64, destroy bool) []byte {
	ctx := context.Background()
	db, tx, doms := setupStepTest(t)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", log.New(), false, 1<<62, make(chan applyResult, 64), nil, make(chan commitmentResult, 64), state.NewLayeredDomainReader(doms, nil, nil), nil)
	require.NoError(t, err)
	defer cc.Stop()
	x := common.Address{19: 0xaa}
	y := common.Address{19: 0xbb}
	accX := accounts.Account{Balance: *uint256.NewInt(5), CodeHash: accounts.EmptyCodeHash}
	accY := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(50), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, x[:], accounts.SerialiseV3(&accX), 1, nil))
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, y[:], accounts.SerialiseV3(&accY), 1, nil))
	slot := accounts.InternKey(common.Hash{31: 1})
	xBal := []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(0)}, {Index: 6, Value: *uint256.NewInt(9)}}
	if !destroy {
		xBal = []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(3)}, {Index: 6, Value: *uint256.NewInt(9)}}
	}
	bl := types.BlockAccessList{
		{
			Address:        x,
			BalanceChanges: xBal,
			NonceChanges:   []*types.NonceChange{{Index: 3, Value: 1}},
			StorageChanges: []types.SlotChanges{{Slot: slot, Changes: []*types.StorageChange{{Index: 3, Value: *uint256.NewInt(7)}}}},
		},
		{Address: y, BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(51)}}},
	}
	req := &blockRequest{blockNum: 2, firstTxNum: 2, lastTxNum: 10, bal: bl}
	vm := state.NewVersionMap(req.bal)
	vm.SetBaseTxNum(req.firstTxNum)
	require.NoError(t, vm.SeedEmptyRemovals(true, false, cc.asOfReader.dr.StateReaderAt(req.firstTxNum)))
	if checkpoint {
		_, flush, err := cc.computeRootFromBAL(ctx, req, vm, -1, 4, commitTarget{blockNum: 2, lastTxNum: 6})
		require.NoError(t, err)
		if flush != nil {
			require.NoError(t, flush())
		}
		_, flush2, err := cc.computeRootFromBAL(ctx, req, vm, 4, 4, commitTarget{blockNum: 2, lastTxNum: 6})
		require.NoError(t, err)
		if flush2 != nil {
			require.NoError(t, flush2())
		}
	}
	rh, _, err := cc.computeRootFromBAL(ctx, req, vm, after, math.MaxUint32, commitTarget{blockNum: 2, lastTxNum: 10})
	require.NoError(t, err)
	return rh
}

func TestBALFoldAfterCheckpointMatchesFullFold(t *testing.T) {
	for _, destroy := range []bool{false, true} {
		t.Run(fmt.Sprintf("removedThenRevived=%t", destroy), func(t *testing.T) {
			full := balFoldRoot(t, false, -1, destroy)
			require.Equal(t, full, balFoldRoot(t, true, -1, destroy), "checkpoint, then a full refold")
			require.Equal(t, full, balFoldRoot(t, true, 4, destroy), "checkpoint, then a fold of the changes after it")
		})
	}
}
