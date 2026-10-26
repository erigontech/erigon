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
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type frameValidationBackend struct {
	remoteproto.ETHBACKENDClient
	payer     common.Address
	balance   uint64
	paymaster bool
	valid     bool
	calls     []common.Hash
}

func (b *frameValidationBackend) FrameValidation(ctx context.Context, req *remoteproto.FrameValidationRequest, opts ...grpc.CallOption) (*remoteproto.FrameValidationReply, error) {
	tx, err := types.DecodeTransaction(req.Transaction)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, tx.Hash())
	var dependencies [][]byte
	if frame, ok := tx.(*types.FrameTransaction); ok && !frame.UsesLegacyNonce() {
		dependencies = append(dependencies, types.FrameNonceManager[:])
	}
	cost := new(uint256.Int).Mul(uint256.NewInt(tx.GetGasLimit()), tx.GetFeeCap())
	return &remoteproto.FrameValidationReply{Valid: b.valid, Payer: b.payer[:], PayerBalance: uint256.NewInt(b.balance).Bytes(), MaxCost: cost.Bytes(), Paymaster: b.paymaster, Dependencies: dependencies}, nil
}

func framePoolSlot(t *testing.T, sender common.Address, tip, fee uint64, keys ...uint256.Int) *TxnSlot {
	t.Helper()
	if len(keys) == 0 {
		keys = []uint256.Int{{}}
	}
	tx := &types.FrameTransaction{
		NonceKeys: keys,
		ChainID:   *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}}},
		Fees:   types.FrameFees{TipCap: *uint256.NewInt(tip), FeeCap: *uint256.NewInt(fee)},
	}
	var data bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&data))
	return &TxnSlot{Txn: tx, Rlp: data.Bytes(), IDHash: tx.Hash(), Size: uint32(data.Len())}
}

func TestFramePoolKeyedNonceIndependent(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	slot := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.Success, reasons[0])
	pool.onSenderStateChange(slot.SenderID, 9, *uint256.NewInt(1_000_000), accounts.EmptyCodeHash, 1_000_000, pool.logger)
	require.Equal(t, 1, pool.pending.Len())
	require.Contains(t, pool.byHash, string(slot.IDHash[:]))
}

func TestFramePoolKeyedNonceIsNotAccountNonce(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.Success, reasons[0])
	_, ok := pool.NonceFromAddress(sender)
	require.False(t, ok)
}

func TestFramePoolRevalidationNonceManager(t *testing.T) {
	for _, tc := range []struct {
		name          string
		action        remoteproto.Action
		key           uint64
		otherSender   bool
		managerSender bool
		direction     remoteproto.Direction
		revalidate    bool
	}{
		{name: "unrelated sender", key: 1, otherSender: true},
		{name: "unselected key", key: 3},
		{name: "first selected key", key: 1, revalidate: true},
		{name: "second selected key", key: 2, revalidate: true},
		{name: "unwind selected key", key: 1, direction: remoteproto.Direction_UNWIND, revalidate: true},
		{name: "unwind unselected key", key: 3, direction: remoteproto.Direction_UNWIND},
		{name: "manager account", action: remoteproto.Action_UPSERT, revalidate: true},
		{name: "manager code", action: remoteproto.Action_CODE, revalidate: true},
		{name: "manager account and code", action: remoteproto.Action_UPSERT_CODE, revalidate: true},
		{name: "manager removed", action: remoteproto.Action_REMOVE, revalidate: true},
		{name: "manager is sender", key: 1, otherSender: true, managerSender: true, revalidate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			if tc.managerSender {
				sender = types.FrameNonceManager
			}
			slot := framePoolSlot(t, sender, 1, 2, uint256.Int{1}, uint256.Int{2})
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, txpoolcfg.Success, reasons[0])
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.calls = nil
			backend.valid = false

			change := &remoteproto.AccountChange{Address: gointerfaces.ConvertAddressToH160(types.FrameNonceManager), Action: tc.action}
			if tc.key != 0 {
				nonceSender := sender
				if tc.otherSender {
					nonceSender = common.Address{2}
				}
				location := types.FrameNonceSlot(nonceSender, *uint256.NewInt(tc.key))
				change.StorageChanges = []*remoteproto.StorageChange{{Location: gointerfaces.ConvertHashToH256(location)}}
			}
			pool.lock.Lock()
			err = pool.revalidateFrames(&remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{Direction: tc.direction, Changes: []*remoteproto.AccountChange{change}}}})
			pool.lock.Unlock()
			require.NoError(t, err)
			if tc.revalidate {
				require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
				require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
				require.Empty(t, pool.frameReservations)
			} else {
				require.Empty(t, backend.calls)
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
				require.Equal(t, slot.frameValidation.MaxCost, pool.frameReservations[backend.payer].cost)
			}
		})
	}
}

func TestFramePoolReservationsAndReplacement(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.chainConfig = pool.chainConfig.Copy()
	pool.chainConfig.BogotaTime = common.NewUint64(0)
	backend := &frameValidationBackend{payer: common.Address{9}, balance: 50_000, valid: true}
	pool.ethBackend = backend
	add := func(sender common.Address, tip, fee uint64) txpoolcfg.DiscardReason {
		var txns TxnSlots
		txns.Append(framePoolSlot(t, sender, tip, fee), sender[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		return reasons[0]
	}
	require.Equal(t, txpoolcfg.Success, add(sender, 1, 2))
	require.Equal(t, txpoolcfg.InsufficientFunds, add(common.Address{2}, 1, 2))
	backend.payer = common.Address{8}
	backend.balance = 100_000
	require.Equal(t, txpoolcfg.Success, add(sender, 2, 4))
	backend.payer = common.Address{9}
	backend.balance = 50_000
	require.Equal(t, txpoolcfg.Success, add(common.Address{2}, 1, 2), "replacement must release the old payer reservation")
}

func TestFramePoolPaymasterLimit(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.chainConfig = pool.chainConfig.Copy()
	pool.chainConfig.BogotaTime = common.NewUint64(0)
	pool.ethBackend = &frameValidationBackend{payer: common.Address{9}, balance: 1_000_000, paymaster: true, valid: true}
	for i, address := range []common.Address{sender, {2}} {
		var txns TxnSlots
		txns.Append(framePoolSlot(t, address, 1, 2), address[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, txpoolcfg.Success, reasons[0])
		} else {
			require.Equal(t, txpoolcfg.InvalidFrame, reasons[0])
		}
	}
}

func TestFramePoolReplacementNeedsFeeIncrease(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.chainConfig = pool.chainConfig.Copy()
	pool.chainConfig.BogotaTime = common.NewUint64(0)
	pool.ethBackend = &frameValidationBackend{payer: common.Address{9}, balance: 1_000_000, valid: true}
	first := framePoolSlot(t, sender, 1, 2)
	var txns TxnSlots
	txns.Append(first, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.Success, reasons[0])
	replacement := framePoolSlot(t, sender, 1, 2)
	replacement.Txn.(*types.FrameTransaction).Frames[0].Data = []byte{1}
	replacement.Txn.(*types.FrameTransaction).TransactionMisc = types.TransactionMisc{}
	replacement.IDHash = replacement.Txn.Hash()
	txns.Txns[0] = replacement
	reasons, err = pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.NotReplaced, reasons[0])
}

func TestFramePoolReplacementRequiresSameKeys(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.Success, reasons[0])
	txns.Txns[0] = framePoolSlot(t, sender, 2, 4, uint256.Int{2})
	reasons, err = pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.InvalidFrame, reasons[0])
	txns.Txns[0] = framePoolSlot(t, sender, 2, 4, uint256.Int{1})
	reasons, err = pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.Success, reasons[0])
}

func TestFramePoolMinedNonceDomains(t *testing.T) {
	for _, tc := range []struct {
		name           string
		pending, mined uint64
		remaining      int
	}{
		{"same key", 1, 1, 0},
		{"different keys", 1, 2, 1},
		{"keyed pending, legacy mined", 1, 0, 1},
		{"legacy pending, keyed mined", 0, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			slot := framePoolSlot(t, sender, 1, 2, *uint256.NewInt(tc.pending))
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, txpoolcfg.Success, reasons[0])
			mined := framePoolSlot(t, sender, 1, 2, *uint256.NewInt(tc.mined))
			mined.SenderID = slot.SenderID
			require.NoError(t, pool.removeMined(pool.all, []*TxnSlot{mined}))
			require.Equal(t, tc.remaining, pool.pending.Len())
		})
	}
}

func keyedFramePoolEnv(t *testing.T) (context.Context, *TxPool, common.Address) {
	t.Helper()
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.chainConfig = pool.chainConfig.Copy()
	pool.chainConfig.BogotaTime = common.NewUint64(0)
	pool.ethBackend = &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	return ctx, pool, sender
}
