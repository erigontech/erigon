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

package txpool

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func recentRootPoolSlot(t *testing.T, sender common.Address, rootSlot, fee uint64) *TxnSlot {
	t.Helper()
	data := make([]byte, 72)
	data[0], data[71] = 1, 2
	binary.BigEndian.PutUint64(data[32:40], rootSlot)
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Target: &types.FrameRecentRootVerifier, Data: data, Limits: mdgas.MdGas{Execution: 20_000}},
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}},
		},
		Fees: types.FrameFees{TipCap: *uint256.NewInt(fee), FeeCap: *uint256.NewInt(fee)},
	}
	return framePoolSlotFromTransaction(t, tx)
}

func TestFramePoolRecentRootAddressAsPaymaster(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.payer, backend.paymaster = types.FrameRecentRootVerifier, true
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveExecution, Limits: mdgas.MdGas{Execution: 100}},
			{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &types.FrameRecentRootVerifier, Limits: mdgas.MdGas{Execution: 100}},
		},
		Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(1)},
	}
	require.NoError(t, protocol.ValidateFrameTransactionPrefix(tx))
	slot := framePoolSlotFromTransaction(t, tx)
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Empty(t, slot.frameValidation.RecentRoots)
	require.Contains(t, slot.frameValidation.Dependencies, types.FrameRecentRootVerifier)
}

func TestFramePoolRecentRootExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		slot *uint64
		keep bool
	}{
		{"oldest usable", common.NewUint64(10 + protocol.RecentRootHistorySlots - 2), true},
		{"expired", common.NewUint64(10 + protocol.RecentRootHistorySlots - 1), false},
		{"current slot after reorg", common.NewUint64(9), false},
		{"future slot after reorg", common.NewUint64(8), false},
		{"missing slot", nil, false},
		{"slot overflow", common.NewUint64(math.MaxUint64), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			initial := newBlockChangeForAccounts(common.Hash{1}, 0)
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(10)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.calls = nil

			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			change.ChangeBatch[0].SlotNumber = tc.slot
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			if tc.keep {
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			} else {
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
				require.Empty(t, pool.all.senderIDFrameTxn)
			}
			require.Empty(t, backend.calls, "age checks must not require EVM simulation")
		})
	}
}

func TestFramePoolRecentRootForkBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		initialTime uint64
		initialSlot uint64
		direction   remoteproto.Direction
		activeHead  bool
		incomplete  bool
		evict       bool
	}{
		{
			name:        "before first Bogota block",
			initialTime: 76, initialSlot: 19, direction: remoteproto.Direction_FORWARD,
			activeHead: false, incomplete: false, evict: false,
		},
		{
			name:        "first activation in batch",
			initialTime: 76, initialSlot: 19, direction: remoteproto.Direction_FORWARD,
			activeHead: true, incomplete: false, evict: false,
		},
		{
			name:        "retained forward history after active snapshot",
			initialTime: 100, initialSlot: 21, direction: remoteproto.Direction_FORWARD,
			activeHead: true, incomplete: false, evict: false,
		},
		{
			name:        "head crosses back before activation",
			initialTime: 100, initialSlot: 21, direction: remoteproto.Direction_UNWIND,
			activeHead: false, incomplete: false, evict: true,
		},
		{
			name:        "activation unwind followed by active head",
			initialTime: 100, initialSlot: 21, direction: remoteproto.Direction_UNWIND,
			activeHead: true, incomplete: false, evict: true,
		},
		{
			name:        "reset crosses back before activation",
			initialTime: 100, initialSlot: 21, direction: remoteproto.Direction_FORWARD,
			activeHead: false, incomplete: true, evict: true,
		},
		{
			name:        "reset before first Bogota block",
			initialTime: 76, initialSlot: 19, direction: remoteproto.Direction_FORWARD,
			activeHead: false, incomplete: true, evict: false,
		},
		{
			name:        "incomplete forward history after active snapshot",
			initialTime: 100, initialSlot: 21, direction: remoteproto.Direction_FORWARD,
			activeHead: true, incomplete: true, evict: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := uint64(time.Now().Unix())
				chainConfig := chain.AllProtocolChanges.Copy()
				chainConfig.BogotaTime = common.NewUint64(now + 100)
				ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
				backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
				pool.ethBackend = backend
				initial := newBlockChangeForAccounts(common.Hash{1}, 0)
				initial.ChangeBatch[0].BlockTime = now + tc.initialTime
				initial.ChangeBatch[0].SlotNumber = common.NewUint64(tc.initialSlot)
				require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				time.Sleep(100 * time.Second)
				slot := recentRootPoolSlot(t, sender, 10, 1)
				var txns TxnSlots
				txns.Append(slot, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

				backend.calls = nil
				change := newBlockChangeForAccounts(common.Hash{2}, 0)
				change.StateChangesIncomplete = tc.incomplete
				change.ChangeBatch[0].BlockTime = now + 88
				change.ChangeBatch[0].SlotNumber = common.NewUint64(20)
				change.ChangeBatch[0].Direction = tc.direction
				if tc.activeHead {
					replacement := newBlockChangeForAccounts(common.Hash{3}, 0)
					replacement.ChangeBatch[0].BlockTime = now + 112
					replacement.ChangeBatch[0].SlotNumber = common.NewUint64(22)
					change.ChangeBatch = append(change.ChangeBatch, replacement.ChangeBatch...)
				}
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.Equal(t, tc.activeHead, pool.headIsBogota)
				if tc.incomplete && !tc.evict {
					require.Equal(t, []common.Hash{slot.IDHash}, backend.calls, "missing history requires revalidation")
				} else {
					require.Empty(t, backend.calls, "activation and age checks do not require simulation")
				}
				if tc.evict {
					require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
					require.Empty(t, pool.frameReservations)
					require.Empty(t, pool.all.senderIDFrameTxn)
				} else {
					require.Contains(t, pool.byHash, string(slot.IDHash[:]))
					require.Equal(t, slot.frameValidation.MaxCost, pool.frameReservations[sender].cost)
				}
			})
		})
	}
}

type frameEvictionErrorView struct {
	kvcache.CacheView
	pool *TxPool
	hash string
	err  error
}

func (v frameEvictionErrorView) Get(address []byte) ([]byte, error) {
	if v.pool.byHash[v.hash] == nil {
		return nil, v.err
	}
	return v.CacheView.Get(address)
}

func TestFramePoolRecentRootActivationUnwindRetry(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now - 1)
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	pool.ethBackend = backend
	initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	initial.ChangeBatch[0].BlockTime = now
	initial.ChangeBatch[0].SlotNumber = common.NewUint64(21)
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	frame := recentRootPoolSlot(t, sender, 10, 1)
	successor := encodeRemoteTxn(t, newTestTxnSlot(1, 0, 1, 1, 21_000))
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	txns.Append(successor, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	backend.calls = nil

	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].BlockTime = now - 2
	change.ChangeBatch[0].SlotNumber = common.NewUint64(20)
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	var unwind TxnSlots
	unwind.Append(recentRootPoolSlot(t, sender, 10, 1), sender[:], false)
	readErr := errors.New("promotion account read failed")
	require.NoError(t, coreDB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		view, err := pool._stateCache.View(ctx, tx)
		require.NoError(t, err)
		failing := frameEvictionErrorView{view, pool, string(frame.IDHash[:]), readErr}
		_, err = pool.applyBlockUpdate(change, unwind, TxnSlots{}, TxnSlots{}, failing, nil)
		require.ErrorIs(t, err, readErr)
		return nil
	}))
	require.NotContains(t, pool.byHash, string(frame.IDHash[:]), "eviction must precede the injected promotion failure")
	require.True(t, pool.stateViewStale)

	require.NoError(t, pool.OnNewBlock(ctx, change, unwind, TxnSlots{}, TxnSlots{}))
	require.NotContains(t, pool.byHash, string(frame.IDHash[:]), "retry must preserve activation-unwind eviction")
	require.Empty(t, pool.frameReservations)
	require.Empty(t, pool.all.senderIDFrameTxn)
	require.Empty(t, backend.calls)
	require.False(t, pool.headIsBogota)
}

func TestFramePoolRecentRootRevalidatedAcrossMissingHistory(t *testing.T) {
	for _, tc := range []struct {
		name       string
		superseded bool
		valid      bool
	}{
		{"delivery gap, root still valid", false, true},
		{"delivery gap, root gone", false, false},
		{"superseded unwind, root still valid", true, true},
		{"superseded unwind, root gone", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := uint64(time.Now().Unix())
			config := chain.AllProtocolChanges.Copy()
			config.BogotaTime = common.NewUint64(now - 1)
			ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
			backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
			pool.ethBackend = backend
			initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
			initial.ChangeBatch[0].BlockTime = now
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(21)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			frame := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			backend.valid, backend.calls = tc.valid, nil
			require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
				return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, 1))
			}))
			if tc.superseded {
				unwind := newBlockChangeForAccounts(common.Hash{2}, 0)
				unwind.StateChangesIncomplete = true
				unwind.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
				unwind.ChangeBatch[0].BlockTime = now
				unwind.ChangeBatch[0].SlotNumber = common.NewUint64(20)
				require.ErrorIs(t, pool.OnNewBlock(ctx, unwind, TxnSlots{}, TxnSlots{}, TxnSlots{}), errStateViewSuperseded)
			}
			replacement := newBlockChangeForAccounts(common.Hash{3}, 0, sender)
			replacement.StateVersionId = 1
			replacement.StateChangesIncomplete = !tc.superseded
			replacement.ChangeBatch[0].BlockTime = now + 12
			replacement.ChangeBatch[0].SlotNumber = common.NewUint64(22)
			require.NoError(t, pool.OnNewBlock(ctx, replacement, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.Equal(t, []common.Hash{frame.IDHash}, backend.calls, "missing history must force revalidation at the replacement head")
			if tc.valid {
				require.Contains(t, pool.byHash, string(frame.IDHash[:]))
			} else {
				require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
				require.Empty(t, pool.frameReservations)
			}
		})
	}
}

func TestFetchRecentRootActivationUnwind(t *testing.T) {
	for _, delivery := range []string{"combined", "superseded unwind", "superseded unwind and reactivation", "superseded activation then unwind", "expired refresh"} {
		t.Run(delivery, func(t *testing.T) {
			now := uint64(time.Now().Unix())
			config := chain.AllProtocolChanges.Copy()
			config.BogotaTime = common.NewUint64(now - 1)
			ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
			fetch := NewFetch(ctx, nil, pool, nil, poolDB, pool.chainID, pool.logger)
			backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
			pool.ethBackend = backend
			initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
			initial.ChangeBatch[0].BlockTime = now
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(21)
			if delivery == "superseded activation then unwind" {
				initial.ChangeBatch[0].BlockTime = now - 2
				initial.ChangeBatch[0].SlotNumber = common.NewUint64(20)
			}
			require.NoError(t, fetch.handleStateChangesRequest(ctx, initial))
			frame := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			backend.calls = nil
			require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
				return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, 1))
			}))

			unwind := newBlockChangeForAccounts(common.Hash{2}, 0)
			unwind.StateChangesIncomplete = true
			unwind.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
			unwind.ChangeBatch[0].BlockTime = now - 2
			unwind.ChangeBatch[0].SlotNumber = common.NewUint64(20)
			replacement := newBlockChangeForAccounts(common.Hash{3}, 0, sender)
			replacement.StateVersionId = 1
			replacement.ChangeBatch[0].BlockTime = now + 12
			replacement.ChangeBatch[0].SlotNumber = common.NewUint64(22)
			switch delivery {
			case "combined":
				replacement.ChangeBatch = append(unwind.ChangeBatch, replacement.ChangeBatch...)
			case "superseded unwind":
				require.ErrorIs(t, fetch.handleStateChangesRequest(ctx, unwind), errStateViewSuperseded)
			case "superseded unwind and reactivation":
				unwind.ChangeBatch = append(unwind.ChangeBatch, replacement.ChangeBatch...)
				require.ErrorIs(t, fetch.handleStateChangesRequest(ctx, unwind), errStateViewSuperseded)
			case "superseded activation then unwind":
				activation := newBlockChangeForAccounts(common.Hash{4}, 0)
				activation.StateChangesIncomplete = true
				activation.ChangeBatch[0].BlockTime = now
				activation.ChangeBatch[0].SlotNumber = common.NewUint64(21)
				require.ErrorIs(t, fetch.handleStateChangesRequest(ctx, activation), errStateViewSuperseded)
				require.ErrorIs(t, fetch.handleStateChangesRequest(ctx, unwind), errStateViewSuperseded)
			case "expired refresh":
				synctest.Test(t, func(t *testing.T) {
					ctrl := gomock.NewController(t)
					stream := remoteproto.NewMockKV_StateChangesClient[*remoteproto.StateChangeBatch](ctrl)
					initial.StateChangesIncomplete = true
					stream.EXPECT().Recv().Return(initial, nil)
					client := remoteproto.NewMockKVClient(ctrl)
					client.EXPECT().StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
						func(streamCtx context.Context, _ *remoteproto.StateChangeRequest, _ ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
							stream.EXPECT().Recv().DoAndReturn(func() (*remoteproto.StateChangeBatch, error) {
								<-streamCtx.Done()
								return unwind, nil
							})
							return stream, nil
						})
					require.ErrorIs(t, fetch.handleStateChanges(ctx, client), errRefreshStateSnapshot)
				})
			}
			require.NoError(t, fetch.handleStateChangesRequest(ctx, replacement))
			require.NotContains(t, pool.byHash, string(frame.IDHash[:]), "activation-unwind eviction must not depend on batch delivery")
			require.Empty(t, pool.frameReservations)
			require.Empty(t, pool.all.senderIDFrameTxn)
			require.Empty(t, backend.calls, "a valid root still requires eviction after an activation unwind")

			reasons, err = pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			next := newBlockChangeForAccounts(common.Hash{5}, 0)
			next.StateVersionId = 1
			next.ChangeBatch[0].BlockTime = now + 24
			next.ChangeBatch[0].SlotNumber = common.NewUint64(23)
			require.NoError(t, fetch.handleStateChangesRequest(ctx, next))
			require.Contains(t, pool.byHash, string(frame.IDHash[:]), "an applied activation unwind must not evict later admissions")
		})
	}
}

func TestFramePoolActivationAfterFailedRevalidation(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now - 1)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	pool.ethBackend = backend
	initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	initial.ChangeBatch[0].BlockTime = now
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	updateCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	backend.onValidate = cancel
	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].BlockTime = now - 2
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.ErrorIs(t, pool.OnNewBlock(updateCtx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}), context.Canceled)
	require.True(t, pool.headIsBogota, "failed revalidation must not commit the activation change")

	backend.onValidate = nil
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.False(t, pool.headIsBogota)
}

func TestFramePoolSupersededUpdateKeepsActivation(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now - 1)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	pool.ethBackend = backend
	initial := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	initial.ChangeBatch[0].BlockTime = now
	require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	backend.onValidate = func() {
		newer := newBlockChangeForAccounts(common.Hash{3}, 1, sender)
		newer.ChangeBatch[0].BlockTime = now + 12
		require.NoError(t, pool.OnNewBlock(ctx, newer, TxnSlots{}, TxnSlots{}, txns))
	}
	older := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	older.ChangeBatch[0].BlockTime = now - 2
	older.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.NoError(t, pool.OnNewBlock(ctx, older, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, common.Hash{3}, pool.stateBlockHash)
	require.True(t, pool.headIsBogota, "completion of an older update must not overwrite the newer activation")
}

func recentRootPoolHeader(blockTime, slot uint64) *types.Header {
	return &types.Header{
		Number: uint256.Int{21}, Time: blockTime, SlotNumber: common.NewUint64(slot),
		BaseFee: uint256.NewInt(1), WithdrawalsHash: new(common.Hash),
		BlobGasUsed: new(uint64), ExcessBlobGas: new(uint64),
		ParentBeaconBlockRoot: new(common.Hash), RequestsHash: new(common.Hash),
		BlockAccessListHash: new(common.Hash),
	}
}

func TestFramePoolStartupDoesNotUnwindActivation(t *testing.T) {
	now := uint64(time.Now().Unix())
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(now)
	ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
	pool.chainID.SetUint64(1)
	backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true, err: errors.New("backend unavailable")}
	pool.ethBackend = backend
	executed := recentRootPoolHeader(now-12, 20)
	executed.Number.SetUint64(20)
	ahead := recentRootPoolHeader(now+12, 22)
	ahead.Number.SetUint64(22)
	require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		for _, header := range []*types.Header{executed, ahead} {
			if err := rawdb.WriteHeader(tx, header); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
				return err
			}
		}
		rawdb.WriteHeadBlockHash(tx, ahead.Hash())
		if err := rawdb.WriteHeadHeaderHash(tx, ahead.Hash()); err != nil {
			return err
		}
		return stages.SaveStageProgress(tx, stages.Execution, executed.Number.Uint64())
	}))
	frame := recentRootPoolSlot(t, sender, 10, 1)
	require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.PoolTransaction, frame.IDHash[:], append(sender[:], frame.Rlp...))
	}))
	pool.MarkStateStale()
	change := newBlockChangeForAccounts(executed.Hash(), 0, sender)
	change.StateChangesIncomplete = true
	head := change.ChangeBatch[0]
	head.BlockHeight, head.BlockTime, head.SlotNumber = executed.Number.Uint64(), executed.Time, executed.SlotNumber
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	pool.started.Store(false)
	require.NoError(t, pool.start(ctx))
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))

	backend.err = nil
	change.StateChangesIncomplete = false
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Contains(t, pool.byHash, string(frame.IDHash[:]), "restoration after the initial reset must not look like an activation unwind")
	require.Equal(t, PendingSubPool, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	require.Len(t, backend.calls, 2)
}

func TestFramePoolReloadRecentRootAge(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headSlot uint64
		keep     bool
	}{
		{"oldest usable", 10 + protocol.RecentRootHistorySlots - 2, true},
		{"expired", 10 + protocol.RecentRootHistorySlots - 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := uint64(time.Now().Unix())
			config := chain.AllProtocolChanges.Copy()
			config.BogotaTime = common.NewUint64(now - 12)
			ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, config)
			pool.chainID.SetUint64(1)
			backend := &frameValidationBackend{err: errors.New("validation backend unavailable")}
			pool.ethBackend = backend
			header := recentRootPoolHeader(now, tc.headSlot)
			require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
				if err := rawdb.WriteHeader(tx, header); err != nil {
					return err
				}
				if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
					return err
				}
				rawdb.WriteHeadBlockHash(tx, header.Hash())
				return stages.SaveStageProgress(tx, stages.Execution, header.Number.Uint64())
			}))
			slot := recentRootPoolSlot(t, sender, 10, 1)
			require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
				return tx.Put(kv.PoolTransaction, slot.IDHash[:], append(sender[:], slot.Rlp...))
			}))

			reset := newBlockChangeForAccounts(header.Hash(), 0, sender)
			reset.StateChangesIncomplete = true
			reset.ChangeBatch[0].BlockHeight = header.Number.Uint64()
			reset.ChangeBatch[0].BlockTime = header.Time
			reset.ChangeBatch[0].SlotNumber = header.SlotNumber
			require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			pool.started.Store(false)
			require.NoError(t, pool.start(ctx))
			mt := pool.byHash[string(slot.IDHash[:])]
			if tc.keep {
				require.NotNil(t, mt)
				require.Zero(t, mt.currentSubPool)
				require.Len(t, backend.calls, 1)
			} else {
				require.Nil(t, mt, "expired roots must be discarded without simulation")
				require.Empty(t, pool.all.senderIDFrameTxn)
				require.Empty(t, backend.calls)
			}
			_, err := pool.flushNoFsync(ctx)
			require.NoError(t, err)
			require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
				persisted, err := tx.Has(kv.PoolTransaction, slot.IDHash[:])
				require.NoError(t, err)
				require.Equal(t, tc.keep, persisted)
				return nil
			}))
		})
	}
}

func TestFramePoolRestoredRecentRootEviction(t *testing.T) {
	for _, tc := range []struct {
		name             string
		validated        bool
		activationUnwind bool
		headSlot         uint64
		keep             bool
	}{
		{"validated activation unwind", true, true, 20, false},
		{"parked activation unwind", false, true, 20, false},
		{"parked expired root", false, false, 10 + protocol.RecentRootHistorySlots - 1, false},
		{"parked usable root", false, false, 22, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := uint64(time.Now().Unix())
				chainConfig := chain.AllProtocolChanges.Copy()
				chainConfig.BogotaTime = common.NewUint64(now)
				ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
				pool.chainID.SetUint64(1)
				backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
				if !tc.validated {
					backend.err = errors.New("validation backend unavailable")
				}
				pool.ethBackend = backend
				header := recentRootPoolHeader(now, 21)
				require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
					if err := rawdb.WriteHeader(tx, header); err != nil {
						return err
					}
					if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
						return err
					}
					rawdb.WriteHeadBlockHash(tx, header.Hash())
					return stages.SaveStageProgress(tx, stages.Execution, header.Number.Uint64())
				}))
				slot := recentRootPoolSlot(t, sender, 10, 1)
				require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.PoolTransaction, slot.IDHash[:], append(sender[:], slot.Rlp...))
				}))
				reset := newBlockChangeForAccounts(header.Hash(), 0, sender)
				reset.StateChangesIncomplete = true
				reset.ChangeBatch[0].BlockHeight = header.Number.Uint64()
				reset.ChangeBatch[0].BlockTime = header.Time
				reset.ChangeBatch[0].SlotNumber = header.SlotNumber
				require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				pool.started.Store(false)
				require.NoError(t, pool.start(ctx))
				mt := pool.byHash[string(slot.IDHash[:])]
				require.NotNil(t, mt)
				if tc.validated {
					require.Equal(t, PendingSubPool, mt.currentSubPool)
				} else {
					require.Zero(t, mt.currentSubPool)
					require.Nil(t, mt.TxnSlot.frameValidation)
				}

				backend.calls, backend.err = nil, nil
				change := newBlockChangeForAccounts(common.Hash{2}, 0)
				head := change.ChangeBatch[0]
				head.BlockTime = now + 12
				head.SlotNumber = common.NewUint64(tc.headSlot)
				if tc.activationUnwind {
					head.BlockTime = now - 12
					head.Direction = remoteproto.Direction_UNWIND
				}
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				if tc.keep {
					require.Contains(t, pool.byHash, string(slot.IDHash[:]))
					require.Equal(t, PendingSubPool, mt.currentSubPool)
					require.Len(t, backend.calls, 1)
				} else {
					require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
					require.Empty(t, pool.frameReservations)
					require.Empty(t, pool.all.senderIDFrameTxn)
					require.Empty(t, backend.calls, "activation and age checks do not require simulation")
				}
			})
		})
	}
}

func TestFramePoolRecentRootDependencies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		action     remoteproto.Action
		selected   bool
		unwind     bool
		revalidate bool
	}{
		{"unrelated root", remoteproto.Action_STORAGE, false, false, false},
		{"verifier balance changed", remoteproto.Action_UPSERT, false, false, false},
		{"verifier balance unwound", remoteproto.Action_UPSERT, false, true, false},
		{"referenced root changed", remoteproto.Action_STORAGE, true, false, true},
		{"referenced root changed with balance", remoteproto.Action_UPSERT, true, false, true},
		{"referenced root cleared by unwind", remoteproto.Action_STORAGE, true, true, true},
		{"verifier code changed", remoteproto.Action_CODE, false, false, true},
		{"verifier account and code changed", remoteproto.Action_UPSERT_CODE, false, false, true},
		{"verifier removed", remoteproto.Action_REMOVE, false, false, true},
		{"verifier deployment unwound", remoteproto.Action_REMOVE, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			require.Len(t, slot.frameValidation.RecentRoots, 1)
			ref := slot.frameValidation.RecentRoots[0]
			key := common.Hash{99}
			if tc.selected {
				key = ref.StorageKey
			}
			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			head := change.ChangeBatch[0]
			head.SlotNumber = common.NewUint64(11)
			head.Changes = []*remoteproto.AccountChange{{
				Address: gointerfaces.ConvertAddressToH160(types.FrameRecentRootVerifier), Action: tc.action,
				StorageChanges: []*remoteproto.StorageChange{{Location: gointerfaces.ConvertHashToH256(key)}},
			}}
			if tc.action == remoteproto.Action_UPSERT || tc.action == remoteproto.Action_UPSERT_CODE {
				account := accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.NewCode(protocol.RecentRootCode).Hash}
				if tc.action == remoteproto.Action_UPSERT_CODE {
					head.Changes[0].Code = []byte{0xfe}
					account.CodeHash = accounts.NewCode(head.Changes[0].Code).Hash
				}
				head.Changes[0].Data = accounts.SerialiseV3(&account)
				if !tc.selected {
					head.Changes[0].StorageChanges = nil
				}
			}
			if tc.unwind {
				head.Direction = remoteproto.Direction_UNWIND
			}
			backend.valid, backend.calls = false, nil
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			if tc.revalidate {
				require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
			} else {
				require.Empty(t, backend.calls)
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			}
		})
	}
}

func TestFramePoolRecentRootDeploymentUnwindWithBalance(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	slot := recentRootPoolSlot(t, sender, 10, 1)
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Len(t, slot.frameValidation.RecentRoots, 1)

	account := accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	change := newBlockChangeForAccounts(common.Hash{2}, 0)
	head := change.ChangeBatch[0]
	head.Direction = remoteproto.Direction_UNWIND
	head.SlotNumber = common.NewUint64(11)
	// The funded account survives deployment rollback; its code and roots do not.
	head.Changes = []*remoteproto.AccountChange{{
		Address: gointerfaces.ConvertAddressToH160(types.FrameRecentRootVerifier),
		Action:  remoteproto.Action_UPSERT,
		Data:    accounts.SerialiseV3(&account),
		StorageChanges: []*remoteproto.StorageChange{{
			Location: gointerfaces.ConvertHashToH256(slot.frameValidation.RecentRoots[0].StorageKey),
		}},
	}}
	backend.valid, backend.calls = false, nil
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
	require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
	require.Empty(t, pool.frameReservations)
}

func TestFramePoolRecentRootReplacement(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	first := recentRootPoolSlot(t, sender, 10, 1)
	replacement := recentRootPoolSlot(t, sender, 20, 2)
	for _, slot := range []*TxnSlot{first, replacement} {
		var txns TxnSlots
		txns.Append(slot, sender[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	}
	require.NotContains(t, pool.byHash, string(first.IDHash[:]))
	require.Contains(t, pool.byHash, string(replacement.IDHash[:]))
	require.Len(t, pool.all.senderIDFrameTxn, 1)
	require.Len(t, replacement.frameValidation.RecentRoots, 1)
	require.Equal(t, uint64(20), replacement.frameValidation.RecentRoots[0].Slot)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.valid, backend.calls = false, nil
	for _, slot := range []*TxnSlot{first, replacement} {
		change := newBlockChangeForAccounts(common.Hash{3}, 0)
		change.ChangeBatch[0].SlotNumber = common.NewUint64(30)
		change.ChangeBatch[0].Changes = []*remoteproto.AccountChange{{
			Address: gointerfaces.ConvertAddressToH160(types.FrameRecentRootVerifier), Action: remoteproto.Action_STORAGE,
			StorageChanges: []*remoteproto.StorageChange{{Location: gointerfaces.ConvertHashToH256(slot.frameValidation.RecentRoots[0].StorageKey)}},
		}}
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		if slot == first {
			require.Empty(t, backend.calls)
			require.Contains(t, pool.byHash, string(replacement.IDHash[:]))
		} else {
			require.Equal(t, []common.Hash{replacement.IDHash}, backend.calls)
			require.NotContains(t, pool.byHash, string(replacement.IDHash[:]))
			require.Empty(t, pool.frameReservations)
		}
	}
}

func TestFramePoolRecentRootIncompleteNotifications(t *testing.T) {
	for _, tc := range []struct {
		name       string
		height     uint64
		incomplete bool
	}{
		{"complete empty block", 1_012, false},
		{"deep reorg without root rewrite", 11, true},
		{"head-only catch-up", 1_200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			initial := newBlockChangeForAccounts(common.Hash{1}, 0)
			initial.ChangeBatch[0].BlockHeight = 1_011
			initial.ChangeBatch[0].SlotNumber = common.NewUint64(1_011)
			require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			slot := recentRootPoolSlot(t, sender, 10, 1)
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			backend.valid, backend.calls = false, nil
			backend.onValidate = func() {
				require.Zero(t, pool.byHash[string(slot.IDHash[:])].currentSubPool, "park before simulation")
			}
			change := newBlockChangeForAccounts(common.Hash{2}, 0)
			change.StateChangesIncomplete = tc.incomplete
			change.ChangeBatch[0].BlockHeight = tc.height
			change.ChangeBatch[0].SlotNumber = common.NewUint64(tc.height)
			require.True(t, slot.frameValidation.RecentRoots[0].ValidAt(tc.height+1), "age checks alone cannot detect the missing root")
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			if tc.incomplete {
				require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
			} else {
				require.Empty(t, backend.calls)
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
			}
		})
	}
}
