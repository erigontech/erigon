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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type frameValidationBackend struct {
	remoteproto.ETHBACKENDClient
	payer          common.Address
	balance        uint64
	paymaster      bool
	valid          bool
	err            error
	calls          []common.Hash
	deadlines      []time.Time
	onValidate     func()
	dependencies   []*typesproto.H160
	creationNonces []*remoteproto.FrameCreationNonce
}

func (b *frameValidationBackend) FrameValidation(ctx context.Context, req *remoteproto.FrameValidationRequest, opts ...grpc.CallOption) (*remoteproto.FrameValidationReply, error) {
	if b.onValidate != nil {
		b.onValidate()
	}
	tx, err := types.DecodeTransaction(req.Transaction)
	if err != nil {
		return nil, err
	}
	b.calls = append(b.calls, tx.Hash())
	deadline, _ := ctx.Deadline()
	b.deadlines = append(b.deadlines, deadline)
	if b.err != nil {
		return nil, b.err
	}
	var expiry uint64
	dependencies := append([]*typesproto.H160(nil), b.dependencies...)
	if frame, ok := tx.(*types.FrameTransaction); ok {
		if !frame.UsesLegacyNonce() {
			dependencies = append(dependencies, gointerfaces.ConvertAddressToH160(types.FrameNonceManager))
		}
		if first := &frame.Frames[0]; first.IsExpiryVerifier() {
			expiry = binary.BigEndian.Uint64(first.Data)
			dependencies = append(dependencies, gointerfaces.ConvertAddressToH160(types.FrameExpiryVerifier))
		}
	}
	cost := new(uint256.Int).Mul(uint256.NewInt(tx.GetGasLimit()), tx.GetFeeCap())
	return &remoteproto.FrameValidationReply{
		Valid: b.valid, Payer: gointerfaces.ConvertAddressToH160(b.payer),
		PayerBalance: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(b.balance)), MaxCost: gointerfaces.ConvertUint256IntToH256(cost),
		Paymaster: b.paymaster, Deadline: expiry, Dependencies: dependencies, CreationNonces: b.creationNonces,
	}, nil
}

func framePoolSlot(t *testing.T, sender common.Address, tip, fee uint64, keys ...uint256.Int) *TxnSlot {
	t.Helper()
	return framePoolSlotWithNonce(t, sender, 0, tip, fee, keys...)
}

func framePoolSlotWithNonce(t *testing.T, sender common.Address, nonce, tip, fee uint64, keys ...uint256.Int) *TxnSlot {
	t.Helper()
	if len(keys) == 0 {
		keys = []uint256.Int{{}}
	}
	tx := &types.FrameTransaction{
		NonceKeys: keys,
		Nonce:     nonce,
		ChainID:   *uint256.NewInt(1), SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}}},
		Fees:   types.FrameFees{TipCap: *uint256.NewInt(tip), FeeCap: *uint256.NewInt(fee)},
	}
	var data bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&data))
	return &TxnSlot{Txn: tx, Nonce: nonce, Rlp: data.Bytes(), IDHash: tx.Hash(), Size: uint32(data.Len())}
}

func TestFramePoolBogotaActivation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		forkTime *uint64
		want     txpoolcfg.DiscardReason
	}{
		{"disabled", nil, txpoolcfg.TypeNotActivated},
		{"genesis", common.NewUint64(0), txpoolcfg.Success},
		{"scheduled", common.NewUint64(math.MaxUint64), txpoolcfg.TypeNotActivated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainConfig := chain.AllProtocolChanges.Copy()
			chainConfig.BogotaTime = tc.forkTime
			_, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
			slot := framePoolSlot(t, sender, 1, 2)
			require.Equal(t, tc.want, pool.validateFrame(slot, true))
		})
	}
	t.Run("activation boundary", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			chainConfig := chain.AllProtocolChanges.Copy()
			chainConfig.BogotaTime = common.NewUint64(uint64(time.Now().Unix()) + 1)
			_, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
			slot := framePoolSlot(t, sender, 1, 2)
			require.Equal(t, txpoolcfg.TypeNotActivated, pool.validateFrame(slot, true))
			time.Sleep(time.Second)
			require.Equal(t, txpoolcfg.Success, pool.validateFrame(slot, true))
		})
	})
}

func TestFramePoolPrecheckBeforeSignatures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		signatures int
		limits     mdgas.MdGas
		want       string
	}{
		{"signatures", 45, mdgas.MdGas{Execution: 100}, "frame signature validation exceeds gas limit"},
		{"prefix execution", 1, mdgas.MdGas{Execution: 293_301}, "frame validation exceeds gas limit"},
		{"prefix state", 1, mdgas.MdGas{Execution: 100, State: 500_001}, "frame validation exceeds gas limit"},
		{"within budget", 1, mdgas.MdGas{Execution: 293_300, State: 500_000}, "invalid frame signature 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := framePoolSlot(t, common.Address{1}, 1, 2).Txn.(*types.FrameTransaction)
			frame.Frames[0].Limits = tc.limits
			frame.Signatures = make([]types.FrameSignature, tc.signatures)
			for i := range frame.Signatures {
				frame.Signatures[i].Scheme = types.FrameSignatureP256
			}
			var encoded bytes.Buffer
			require.NoError(t, frame.MarshalBinary(&encoded))
			for _, withSender := range []bool{false, true} {
				t.Run(fmt.Sprintf("gossip/withSender=%t", withSender), func(t *testing.T) {
					fetch := NewFetch(t.Context(), nil, nil, nil, nil, frame.ChainID, log.New())
					fetch.pooledTxnsParseCtx.WithSender(withSender)
					var slot TxnSlot
					var sender common.Address
					_, err := fetch.pooledTxnsParseCtx.ParseTransaction(encoded.Bytes(), 0, &slot, sender[:], false, true, nil)
					require.ErrorContains(t, err, tc.want)
				})
			}
			t.Run("RPC", func(t *testing.T) {
				pool := &addMockTxPool{}
				server := NewGrpcServer(t.Context(), pool, mdbxtest.NewTestPoolDB(t), nil, frame.ChainID, log.New())
				reply, err := server.Add(t.Context(), &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
				require.NoError(t, err)
				require.Len(t, reply.Errors, 1)
				require.Contains(t, reply.Errors[0], tc.want)
				require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID}, reply.Imported)
				require.Zero(t, pool.addLocalSlotsLen)
			})
		})
	}
}

func TestFramePoolDirectAdmissionChecksSignatures(t *testing.T) {
	for _, name := range []string{"constructed", "replaced parsed transaction"} {
		t.Run(name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			slot := framePoolSlot(t, sender, 1, 2)
			frame := slot.Txn.(*types.FrameTransaction)
			if name == "replaced parsed transaction" {
				parser := NewTxnParseContext(frame.ChainID)
				var parsed TxnSlot
				var parsedSender common.Address
				_, err := parser.ParseTransaction(slot.Rlp, 0, &parsed, parsedSender[:], false, true, nil)
				require.NoError(t, err)
				parsed.Txn = frame
				slot = &parsed
			}
			frame.Signatures = []types.FrameSignature{{Signature: []byte{1}}}
			require.NoError(t, frame.Validate())
			frame.Signatures = append(frame.Signatures, types.FrameSignature{Scheme: types.FrameSignatureSecp256k1, Signature: make([]byte, 65)})
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.InvalidFrame}, reasons)
			require.Empty(t, pool.ethBackend.(*frameValidationBackend).calls)
		})
	}
}

func TestFramePoolPrecheckPanicReleasesLock(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], false)
	// Force a panic during the locked policy checks.
	pool.frameRejections = nil
	require.Panics(t, func() { pool.prepareFrames(ctx, txns) })
	unlocked := pool.lock.TryLock()
	if unlocked {
		pool.lock.Unlock()
	}
	require.True(t, unlocked, "panic recovery must be able to acquire the pool lock")
}

func TestFramePoolBudgetDoesNotRestrictMinedTransactions(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := make([]byte, 64)
	key.X.FillBytes(pub[:32])
	key.Y.FillBytes(pub[32:])
	signer := common.BytesToAddress(crypto.Keccak256(pub)[12:])
	message := common.Hash{1}
	r, s, err := ecdsa.Sign(rand.Reader, key, message[:])
	require.NoError(t, err)
	n := key.Curve.Params().N
	if s.Cmp(new(big.Int).Rsh(new(big.Int).Set(n), 1)) > 0 {
		s.Sub(n, s)
	}
	signature := make([]byte, 128)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:64])
	copy(signature[64:], pub)
	frame := framePoolSlot(t, sender, 1, 2).Txn.(*types.FrameTransaction)
	frame.Signatures = make([]types.FrameSignature, 45)
	for i := range frame.Signatures {
		frame.Signatures[i] = types.FrameSignature{Scheme: types.FrameSignatureP256, Signer: signer[:], Message: message[:], Signature: signature}
	}
	require.NoError(t, frame.Validate())
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	fetch := NewFetch(ctx, nil, pool, nil, nil, frame.ChainID, log.New())
	var slot TxnSlot
	var parsedSender common.Address
	_, err = fetch.stateChangesParseCtx.ParseTransaction(encoded.Bytes(), 0, &slot, parsedSender[:], false, false, nil)
	require.NoError(t, err)
	require.Equal(t, sender, parsedSender)

	var txns TxnSlots
	txns.Append(&slot, parsedSender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.InvalidFrame}, reasons)
	require.Empty(t, pool.ethBackend.(*frameValidationBackend).calls)
}

func TestFramePoolKeyedAndOrdinaryNoncesCoexist(t *testing.T) {
	for _, tc := range []struct {
		name       string
		frameFirst bool
		sequence   uint64
	}{
		{"frame first, same sequence", true, 0},
		{"ordinary first, same sequence", false, 0},
		{"frame first, different sequence", true, 5},
		{"ordinary first, different sequence", false, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			frame := framePoolSlotWithNonce(t, sender, tc.sequence, 1, 2, uint256.Int{1})
			ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
			ordinary.IDHash[0] = 2
			slots := []*TxnSlot{ordinary, frame}
			if tc.frameFirst {
				slots[0], slots[1] = slots[1], slots[0]
			}
			for _, slot := range slots {
				var txns TxnSlots
				txns.Append(slot, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			}
			require.Contains(t, pool.byHash, string(frame.IDHash[:]))
			require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
			require.Equal(t, 2, pool.pending.Len())
			require.Equal(t, 2, pool.all.count(frame.SenderID))
			nonce, ok := pool.NonceFromAddress(sender)
			require.True(t, ok)
			require.Zero(t, nonce)
		})
	}
}

func TestFramePoolDelegatedSenderLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		frameFirst bool
		batch      bool
	}{
		{name: "ordinary first"},
		{name: "frame first", frameFirst: true},
		{name: "ordinary first in batch", batch: true},
		{name: "frame first in batch", frameFirst: true, batch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainConfig := chain.AllProtocolChanges.Copy()
			chainConfig.BogotaTime = common.NewUint64(0)
			ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, testDelegationCodeHash(), chainConfig)
			pool.ethBackend = &frameValidationBackend{payer: sender, balance: common.Ether, valid: true}
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
			ordinary.IDHash[0] = 2
			slots := []*TxnSlot{ordinary, frame}
			if tc.frameFirst {
				slots[0], slots[1] = slots[1], slots[0]
			}
			var txns TxnSlots
			for _, slot := range slots {
				txns.Append(slot, sender[:], true)
			}
			var reasons []txpoolcfg.DiscardReason
			if tc.batch {
				var err error
				reasons, err = pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
			} else {
				for _, slot := range slots {
					var single TxnSlots
					single.Append(slot, sender[:], true)
					result, err := pool.AddLocalTxns(ctx, single)
					require.NoError(t, err)
					reasons = append(reasons, result...)
				}
			}
			wantReasons := []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.DelegatedTxnLimit}
			kept := slots[0]
			if tc.batch {
				kept = ordinary
				if tc.frameFirst {
					wantReasons[0], wantReasons[1] = wantReasons[1], wantReasons[0]
				}
			}
			require.Equal(t, wantReasons, reasons)
			require.Len(t, pool.byHash, 1)
			require.Contains(t, pool.byHash, string(kept.IDHash[:]))
			require.Equal(t, 1, pool.pending.Len())
			if kept == frame {
				require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[sender].cost)
			} else {
				require.Empty(t, pool.frameReservations)
			}
		})
	}
}

func TestFramePoolNewlyDelegatedSender(t *testing.T) {
	for _, nonce := range []uint64{1, 2} {
		t.Run(fmt.Sprintf("account nonce %d", nonce), func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			ordinary := newTestTxnSlot(1, 0, 1, 2, 21_000)
			ordinary.IDHash[0] = 2
			var txns TxnSlots
			txns.Append(ordinary, sender[:], true)
			txns.Append(frame, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
			require.Len(t, pool.byHash, 2)

			change := newBlockChangeForAccounts(common.Hash{1}, nonce, sender)
			account := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: testDelegationCodeHash()}
			change.ChangeBatch[0].Changes[0].Data = accounts.SerialiseV3(&account)
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

			require.Len(t, pool.byHash, 1)
			require.Equal(t, 1, pool.pending.Len())
			if nonce == 1 {
				require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
				require.Empty(t, pool.frameReservations)
				reason, ok := pool.discardReasonsLRU.Get(string(frame.IDHash[:]))
				require.True(t, ok)
				require.Equal(t, txpoolcfg.DelegatedTxnLimit, reason)
			} else {
				require.Contains(t, pool.byHash, string(frame.IDHash[:]))
				require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[sender].cost)
			}
		})
	}
}

func TestFramePoolSharesOrdinaryBalance(t *testing.T) {
	for _, tc := range []struct {
		name       string
		frameFirst bool
		batch      bool
	}{
		{name: "ordinary first"},
		{name: "frame first", frameFirst: true},
		{name: "ordinary first in batch", batch: true},
		{name: "frame first in batch", frameFirst: true, batch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, sponsored := range []bool{false, true} {
				name := "self paid"
				if sponsored {
					name = "sponsored"
				}
				t.Run(name, func(t *testing.T) {
					ctx, pool, payer := keyedFramePoolEnv(t)
					pool.ethBackend.(*frameValidationBackend).balance = common.Ether
					sender := payer
					if sponsored {
						sender = common.Address{2}
					}
					frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
					ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
					ordinary.IDHash[0] = 2
					ordinary.Txn.GetValue().SetUint64(common.Ether - 42_000)
					var txns TxnSlots
					txns.Append(ordinary, payer[:], true)
					want := []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.InsufficientFunds}
					wantOrdinaryPool := PendingSubPool
					if tc.frameFirst {
						txns = TxnSlots{}
						txns.Append(frame, sender[:], true)
						txns.Append(ordinary, payer[:], true)
						want[1] = txpoolcfg.Success
						wantOrdinaryPool = QueuedSubPool
					} else {
						txns.Append(frame, sender[:], true)
					}
					if tc.batch {
						reasons, err := pool.AddLocalTxns(ctx, txns)
						require.NoError(t, err)
						require.Equal(t, want, reasons)
					} else {
						for i, txn := range txns.Txns {
							var single TxnSlots
							single.Append(txn, txns.Senders.At(i), true)
							reasons, err := pool.AddLocalTxns(ctx, single)
							require.NoError(t, err)
							require.Equal(t, []txpoolcfg.DiscardReason{want[i]}, reasons)
						}
					}
					require.Equal(t, wantOrdinaryPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
					require.Equal(t, 1, pool.pending.Len(), "the payer cannot fund both transactions")
				})
			}
		})
	}
}

func TestFramePoolReservationReleasePromotesOrdinary(t *testing.T) {
	for _, action := range []string{"replacement", "mined", "expired", "revalidation rejected", "revalidation payer change", "pool overflow"} {
		t.Run(action, func(t *testing.T) {
			ctx, pool, payer := keyedFramePoolEnv(t)
			deadline := uint64(time.Now().Add(time.Hour).Unix())
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.balance = common.Ether
			sender := common.Address{2}
			tx := &types.FrameTransaction{
				NonceKeys: []uint256.Int{{1}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
				Frames: []types.Frame{
					{Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: binary.BigEndian.AppendUint64(nil, deadline), Limits: mdgas.MdGas{Execution: 5_000}},
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}},
				},
				Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)},
			}
			var encoded bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&encoded))
			frame := &TxnSlot{Txn: tx, IDHash: tx.Hash(), Rlp: encoded.Bytes(), Size: uint32(encoded.Len())}
			ordinary := newTestTxnSlot(0, 0, 1, 4, 21_000)
			ordinary.IDHash[0] = 2
			ordinary.Txn.GetValue().SetUint64(common.Ether - 84_000)
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			txns.Append(ordinary, payer[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
			require.Equal(t, QueuedSubPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)

			change := newBlockChangeForAccounts(common.Hash{1}, 0)
			var mined TxnSlots
			switch action {
			case "replacement":
				backend.payer = common.Address{3}
				var replacement TxnSlots
				replacement.Append(framePoolSlot(t, sender, 2, 4, uint256.Int{1}), sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, replacement)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			case "mined":
				mined.Append(frame, sender[:], false)
			case "expired":
				change.ChangeBatch[0].BlockTime = deadline + 1
			case "revalidation rejected":
				backend.valid = false
				change = newBlockChangeForAccounts(common.Hash{1}, 0, sender)
			case "revalidation payer change":
				backend.payer = common.Address{3}
				change = newBlockChangeForAccounts(common.Hash{1}, 0, sender)
			case "pool overflow":
				pool.baseFee.limit = 0
				change.PendingBlockBaseFee = 3
			}
			if action != "replacement" {
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, mined))
			}
			require.NotContains(t, pool.frameReservations, payer)
			require.Equal(t, PendingSubPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
		})
	}
}

func TestFramePoolRevalidationRefreshesSenderNonceChain(t *testing.T) {
	for _, action := range []string{"revalidation rejected", "expired", "parked pool overflow"} {
		t.Run(action, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			deadline := uint64(time.Now().Add(time.Hour).Unix())
			backend := pool.ethBackend.(*frameValidationBackend)
			payer := common.Address{0x44}
			backend.payer = payer
			tx := &types.FrameTransaction{
				ChainID: *uint256.NewInt(1), NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
				Frames: []types.Frame{
					{Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: binary.BigEndian.AppendUint64(nil, deadline), Limits: mdgas.MdGas{Execution: 5_000}},
					{Mode: types.FrameVerify, Flags: types.FrameApproveExecution, Limits: mdgas.MdGas{Execution: 100}},
					{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &payer, Limits: mdgas.MdGas{Execution: 100}},
				},
				Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)},
			}
			var encoded bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&encoded))
			frame := &TxnSlot{Txn: tx, IDHash: tx.Hash(), Rlp: encoded.Bytes(), Size: uint32(encoded.Len())}
			ordinary := newTestTxnSlot(1, 0, 1, 2, 21_000)
			ordinary.IDHash[0] = 0x77
			var ordinaryRLP bytes.Buffer
			require.NoError(t, ordinary.Txn.MarshalBinary(&ordinaryRLP))
			ordinary.Rlp = ordinaryRLP.Bytes()
			ordinary.Size = uint32(ordinaryRLP.Len())
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			txns.Append(ordinary, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
			require.Equal(t, 2, pool.pending.Len())

			change := newBlockChangeForAccounts(common.Hash{1}, 0, payer)
			change.ChangeBatch[0].BlockHeight = 1
			switch action {
			case "revalidation rejected":
				backend.valid = false
			case "expired":
				change.ChangeBatch[0].BlockTime = deadline + 1
			case "parked pool overflow":
				pool.pending.limit, pool.baseFee.limit, pool.queued.limit = 1, 0, 1
				backend.err = context.DeadlineExceeded
			}
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			wantSelected := 0
			if action == "parked pool overflow" {
				require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
				backend.err = nil
				otherSender := common.Address{0x55}
				var other TxnSlots
				other.Append(framePoolSlot(t, otherSender, 1, 2, uint256.Int{1}), otherSender[:], true)
				reasons, addErr := pool.AddLocalTxns(ctx, other)
				require.NoError(t, addErr)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				require.Contains(t, pool.byHash, string(frame.IDHash[:]))
				require.NotContains(t, pool.byHash, string(ordinary.IDHash[:]), "evict the blocked successor before its parked predecessor")
				wantSelected = 1
			} else {
				require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
				mt := pool.byHash[string(ordinary.IDHash[:])]
				require.NotNil(t, mt)
				require.Equal(t, QueuedSubPool, mt.currentSubPool, "nonce 1 cannot execute without nonce 0")
				require.Zero(t, mt.subPool&NoNonceGaps)
			}
			var selected TxnsRlp
			_, count, err := pool.best(ctx, 10, &selected, 1, nil,
				mdgas.FullMdGas{MdGas: mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}}, nil, 1_000_000)
			require.NoError(t, err)
			require.Equal(t, wantSelected, count)
			require.NotContains(t, selected.ParsedTxn, ordinary.Txn)
		})
	}
}

func TestFramePoolSharedBalanceReplacements(t *testing.T) {
	ctx, pool, payer := keyedFramePoolEnv(t)
	pool.ethBackend.(*frameValidationBackend).balance = common.Ether
	sender := common.Address{2}
	add := func(slot *TxnSlot, sender common.Address) txpoolcfg.DiscardReason {
		var txns TxnSlots
		txns.Append(slot, sender[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		return reasons[0]
	}
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	require.Equal(t, txpoolcfg.Success, add(frame, sender))
	frameCost := frame.frameValidation.MaxCost.Uint64()
	for _, tc := range []struct {
		fee      uint64
		cost     uint64
		wantPool SubPoolType
	}{
		{2, common.Ether - frameCost, PendingSubPool},
		{4, common.Ether, QueuedSubPool},
		{8, common.Ether - frameCost, PendingSubPool},
	} {
		ordinary := newTestTxnSlot(0, 0, tc.fee, tc.fee, 21_000)
		ordinary.IDHash[0] = byte(tc.fee)
		ordinary.Txn.GetValue().SetUint64(tc.cost - 21_000*tc.fee)
		require.Equal(t, txpoolcfg.Success, add(ordinary, payer))
		require.Equal(t, tc.wantPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
		require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[payer].cost)
	}

	replacement := framePoolSlot(t, sender, 2, 4, uint256.Int{1})
	require.Equal(t, txpoolcfg.InsufficientFunds, add(replacement, sender))
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))
	require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[payer].cost)

	ordinary := newTestTxnSlot(0, 0, 16, 16, 21_000)
	ordinary.IDHash[0] = 16
	ordinary.Txn.GetValue().SetUint64(common.Ether - 2*frameCost - 21_000*16)
	require.Equal(t, txpoolcfg.Success, add(ordinary, payer))
	require.Equal(t, txpoolcfg.Success, add(replacement, sender))
	require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
	require.Equal(t, replacement.frameValidation.MaxCost, pool.frameReservations[payer].cost)
	require.Equal(t, 2, pool.pending.Len())
}

func TestFramePoolReservationExcludesReplacedOrdinary(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.ethBackend.(*frameValidationBackend).balance = common.Ether
	frame := framePoolSlot(t, sender, 2, 4)
	frameCost := frame.GetGas() * frame.GetFeeCap().Uint64()
	first := newTestTxnSlot(0, 0, 1, 2, 21_000)
	first.IDHash[0] = 1
	second := newTestTxnSlot(1, 0, 1, 2, 21_000)
	second.IDHash[0] = 2
	second.Txn.GetValue().SetUint64(common.Ether - frameCost - 42_000)
	var txns TxnSlots
	txns.Append(first, sender[:], true)
	txns.Append(second, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	txns = TxnSlots{}
	txns.Append(frame, sender[:], true)
	reasons, err = pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.NotContains(t, pool.byHash, string(first.IDHash[:]))
	require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[sender].cost)
	require.Equal(t, 2, pool.pending.Len())
}

func TestFramePoolKeyedNonceDoesNotConflictWithAuthorization(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	ordinary := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
	ordinary.IDHash[0] = 2
	frameSender := common.Address{2}
	ordinary.AuthAndNonces = []AuthAndNonce{{frameSender, 0}}
	var txns TxnSlots
	txns.Append(ordinary, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	reservation := pool.auths[AuthAndNonce{frameSender, 0}]
	require.NotNil(t, reservation)
	txns = TxnSlots{}
	txns.Append(framePoolSlot(t, frameSender, 1, 2, uint256.Int{1}), frameSender[:], true)
	reasons, err = pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Same(t, reservation, pool.auths[AuthAndNonce{frameSender, 0}])
}

func TestFramePoolKeyedNonceIndependent(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	slot := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	var txns TxnSlots
	txns.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, txpoolcfg.Success, reasons[0])
	pool.onSenderStateChange(slot.SenderID, 9, *uint256.NewInt(1_000_000), accounts.EmptyCodeHash, 1_000_000)
	require.Equal(t, 1, pool.pending.Len())
	require.Contains(t, pool.byHash, string(slot.IDHash[:]))
}

func TestFramePoolValidationErrorIsolated(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.err = errors.New("validation backend unavailable")
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
			ordinary.IDHash[0] = 2
			var txns TxnSlots
			txns.Append(frame, sender[:], local)
			txns.Append(ordinary, sender[:], local)
			if local {
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, txpoolcfg.FrameValidationUnavailable, reasons[0])
				require.Equal(t, txpoolcfg.Success, reasons[1])
			} else {
				pool.started.Store(true)
				pool.AddRemoteTxns(ctx, txns, nil, nil)
				require.NoError(t, pool.processRemoteTxns(ctx))
				require.Len(t, pool.remoteFrames.ready, 1)
				pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready)
				require.Empty(t, pool.unprocessedRemoteTxns.Txns)
				require.Empty(t, pool.unprocessedRemoteByHash)
			}
			require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
			require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
			require.Empty(t, pool.frameReservations)
		})
	}
}

func TestFramePoolRejectionCache(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			pool.started.Store(true)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.valid = false
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			add := func(want txpoolcfg.DiscardReason) {
				var txns TxnSlots
				txns.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], local)
				if local {
					reasons, err := pool.AddLocalTxns(ctx, txns)
					require.NoError(t, err)
					require.Equal(t, []txpoolcfg.DiscardReason{want}, reasons)
				} else {
					pool.AddRemoteTxns(ctx, txns, nil, nil)
					require.NoError(t, pool.processRemoteTxns(ctx))
					require.Len(t, pool.remoteFrames.ready, 1)
					pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready)
					require.Empty(t, pool.unprocessedRemoteTxns.Txns)
					require.Empty(t, pool.unprocessedRemoteByHash)
				}
			}
			known := func(want bool) {
				require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
					got, err := pool.IdHashKnown(tx, frame.IDHash[:])
					require.NoError(t, err)
					require.Equal(t, want, got)
					return nil
				}))
			}

			add(txpoolcfg.InvalidFrame)
			add(txpoolcfg.InvalidFrame)
			require.Len(t, backend.calls, 1, "the same rejected frame must not be simulated again against unchanged state")
			require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
			known(true)

			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			known(false)
			backend.valid = true
			add(txpoolcfg.Success)
			require.Len(t, backend.calls, 2)
			require.Contains(t, pool.byHash, string(frame.IDHash[:]))
		})
	}
}

func TestFramePoolRejectionCacheWithinBatch(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.valid = false
	var txns TxnSlots
	for range 2 {
		txns.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], true)
	}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.InvalidFrame, txpoolcfg.InvalidFrame}, reasons)
	require.Len(t, backend.calls, 1, "duplicates prepared before simulation must share the rejection")
}

func TestFramePoolSenderPolicyRejectionCache(t *testing.T) {
	for _, nonce := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("sequence %d", nonce), func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 2, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			add := func(key, nonce uint64, want txpoolcfg.DiscardReason) *TxnSlot {
				t.Helper()
				frame := framePoolSlotWithNonce(t, sender, nonce, 1, 2, uint256.Int{key})
				var txns TxnSlots
				txns.Append(frame, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{want}, reasons)
				return frame
			}
			first := add(1, 0, txpoolcfg.Success)
			rejected := add(2, nonce, txpoolcfg.InvalidFrame)
			add(2, nonce, txpoolcfg.InvalidFrame)
			require.Len(t, backend.calls, 1, "sender conflicts must be rejected before simulation")
			require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, rejected.IDHash[:])
				require.NoError(t, err)
				require.True(t, known)
				return nil
			}))
			pool.lock.Lock()
			mt := pool.byHash[string(first.IDHash[:])]
			pool.removeFromSubPool(mt, "test")
			pool.discardLocked(mt, txpoolcfg.Mined)
			pool.lock.Unlock()
			require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, rejected.IDHash[:])
				require.NoError(t, err)
				require.False(t, known)
				return nil
			}))
			add(2, nonce, txpoolcfg.Success)
			require.Len(t, backend.calls, 2, "removing the conflicting frame must allow retry without waiting for a block")
		})
	}
}

func TestFramePoolSenderConflictClearedDuringAdmission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		key         uint64
		senderNonce uint64
		cached      bool
		expired     bool
		want        txpoolcfg.DiscardReason
	}{
		{name: "fresh conflict", key: 2, want: txpoolcfg.Success},
		{name: "cached conflict", key: 2, cached: true, want: txpoolcfg.Success},
		{name: "expired deadline", key: 2, expired: true, want: txpoolcfg.FrameValidationUnavailable},
		{name: "consumed legacy nonce", senderNonce: 1, expired: true, want: txpoolcfg.NonceTooLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, tc.senderNonce, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			blocker := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			var txns TxnSlots
			txns.Append(blocker, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			candidate := framePoolSlot(t, sender, 1, 2, uint256.Int{tc.key})
			txns.Txns[0] = candidate
			if tc.cached {
				reasons, err = pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.InvalidFrame}, reasons)
			}
			otherSender := common.Address{2}
			txns.Append(framePoolSlot(t, otherSender, 1, 2, uint256.Int{1}), otherSender[:], true)
			generation := pool.stateGeneration
			synctest.Test(t, func(t *testing.T) {
				backend.onValidate = func() {
					backend.onValidate = nil
					pool.lock.Lock()
					mt := pool.byHash[string(blocker.IDHash[:])]
					pool.removeFromSubPool(mt, "test")
					pool.discardLocked(mt, txpoolcfg.PendingPoolOverflow)
					pool.lock.Unlock()
					if tc.expired {
						time.Sleep(frameValidationTimeout)
						synctest.Wait()
					}
				}
				reasons, err = pool.AddLocalTxns(t.Context(), txns)
				require.NoError(t, err)
				require.Equal(t, generation, pool.stateGeneration)
				require.Equal(t, []txpoolcfg.DiscardReason{tc.want, txpoolcfg.Success}, reasons)
				if tc.want == txpoolcfg.Success {
					require.Contains(t, backend.calls, common.Hash(candidate.IDHash), "a cleared conflict must retry simulation before admission")
					require.Contains(t, pool.byHash, string(candidate.IDHash[:]))
				} else {
					require.NotContains(t, backend.calls, common.Hash(candidate.IDHash))
					require.NotContains(t, pool.byHash, string(candidate.IDHash[:]))
				}
			})
		})
	}
}

func TestFramePoolConsumedNonceBeforeSenderConflict(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			blocker := framePoolSlotWithNonce(t, sender, 1, 1, 2)
			if cached {
				// A keyed frame can remain pooled while the account nonce advances.
				blocker = framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			} else {
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			}
			var txns TxnSlots
			txns.Append(blocker, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			candidate := framePoolSlot(t, sender, 1, 2)
			server := NewGrpcServer(ctx, pool, pool.poolDB, nil, *candidate.Txn.GetChainID(), log.New())
			submit := func(want txpoolproto.ImportResult, reason txpoolcfg.DiscardReason) {
				t.Helper()
				reply, err := server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{candidate.Rlp}})
				require.NoError(t, err)
				require.Equal(t, []string{reason.String()}, reply.Errors)
				require.Equal(t, []txpoolproto.ImportResult{want}, reply.Imported)
			}
			if cached {
				submit(txpoolproto.ImportResult_INVALID, txpoolcfg.InvalidFrame)
				rejection, ok := pool.frameRejections.Get(string(candidate.IDHash[:]))
				require.True(t, ok)
				require.Equal(t, common.Hash(blocker.IDHash), rejection.blocker)
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.Contains(t, pool.byHash, string(blocker.IDHash[:]))
			}
			calls := len(backend.calls)
			for range 2 {
				submit(txpoolproto.ImportResult_STALE, txpoolcfg.NonceTooLow)
			}
			require.Len(t, backend.calls, calls, "consumed account nonces do not need simulation")
			require.NotContains(t, pool.byHash, string(candidate.IDHash[:]))
			if cached {
				change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
				change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				submit(txpoolproto.ImportResult_INVALID, txpoolcfg.InvalidFrame)
			}
		})
	}
}

type failingFrameCacheView struct {
	kvcache.CacheView
	err error
}

func (v failingFrameCacheView) Get([]byte) ([]byte, error) { return nil, v.err }

func TestFramePoolFailedBlockUpdateWakesWaiters(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		generation, height := pool.stateGeneration, pool.lastSeenBlock.Load()
		done := make(chan bool, 1)
		go func() { done <- pool.waitForFrameStateChange(ctx, generation) }()
		synctest.Wait()
		readErr := errors.New("state read failed")
		change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
		change.ChangeBatch[0].BlockHeight = height + 1
		_, err := pool.applyBlockUpdate(change, TxnSlots{}, TxnSlots{}, TxnSlots{}, failingFrameCacheView{err: readErr}, nil)
		require.ErrorIs(t, err, readErr)
		require.Equal(t, height, pool.lastSeenBlock.Load())
		synctest.Wait()
		select {
		case changed := <-done:
			require.True(t, changed)
		default:
			t.Fatal("a failed block update must wake waiters when the generation changes")
		}
	})
}

func TestFramePoolValidationReleasesLock(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.onValidate = func() {
				require.True(t, pool.lock.TryLock(), "frame simulation must not hold the pool lock")
				pool.lock.Unlock()
			}
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			var txns TxnSlots
			txns.Append(frame, sender[:], local)
			if local {
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			} else {
				pool.started.Store(true)
				pool.AddRemoteTxns(ctx, txns, nil, nil)
				require.NoError(t, pool.processRemoteTxns(ctx))
			}
			if !local {
				require.Len(t, pool.remoteFrames.ready, 1)
				pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready)
			}
			require.Contains(t, pool.byHash, string(frame.IDHash[:]))
			require.Len(t, backend.calls, 1)
		})
	}
}

func TestFramePoolValidationUsesFreshState(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.onValidate = func() {
		if len(backend.calls) == 0 {
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			backend.valid = false
		} else {
			backend.valid = true
		}
	}
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Len(t, backend.calls, 2, "a block change must invalidate both positive and negative validation results")
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))
}

func TestFramePoolValidationPinsHead(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	hash := common.Hash{1}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(hash, 1, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	backend := pool.ethBackend.(*frameValidationBackend)
	pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
		require.Equal(t, gointerfaces.ConvertHashToH256(hash), req.ExpectedBlockHash)
		return backend.FrameValidation(ctx, req)
	}}
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
}

func TestFramePoolAdmissionWaitsForHeadNotification(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			_, pool, sender := keyedFramePoolEnv(t)
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				oldHead, newHead := common.Hash{1}, common.Hash{2}
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(oldHead, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
				backend := pool.ethBackend.(*frameValidationBackend)
				var heads []common.Hash
				var deadlines []time.Time
				pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
					head := common.Hash(gointerfaces.ConvertH256ToHash(req.ExpectedBlockHash))
					heads = append(heads, head)
					deadline, _ := ctx.Deadline()
					deadlines = append(deadlines, deadline)
					if head != newHead {
						return nil, status.Error(codes.Aborted, "frame validation head changed")
					}
					return backend.FrameValidation(ctx, req)
				}}
				frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
				var txns TxnSlots
				txns.Append(frame, sender[:], local)
				var reasons []txpoolcfg.DiscardReason
				var err error
				done := make(chan struct{})
				if local {
					go func() {
						defer close(done)
						reasons, err = pool.AddLocalTxns(ctx, txns)
					}()
				} else {
					pool.started.Store(true)
					pool.AddRemoteTxns(ctx, txns, nil, nil)
					require.NoError(t, pool.processRemoteTxns(ctx))
					go func() {
						defer close(done)
						pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready)
					}()
				}
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("admission returned before the pool received the new head")
				default:
				}
				require.Equal(t, []common.Hash{oldHead}, heads, "wait for the notification instead of repeating requests against the old head")
				require.Empty(t, pool.frameValidationGate, "waiting for state must release the simulation gate")
				require.True(t, pool.lock.TryLock(), "waiting for state must release the pool lock")
				pool.lock.Unlock()
				time.Sleep(time.Second)
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(newHead, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
				<-done
				require.NoError(t, err)
				if local {
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				}
				require.Equal(t, []common.Hash{oldHead, newHead}, heads)
				require.Equal(t, deadlines[0], deadlines[1], "a head change must not restart the admission deadline")
				require.Contains(t, pool.byHash, string(frame.IDHash[:]))
				require.Empty(t, pool.remoteFrames.pending)
			})
		})
	}
}

func TestFramePoolValidationBatchDeadline(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	var txns TxnSlots
	for _, address := range []common.Address{sender, {2}} {
		txns.Append(framePoolSlot(t, address, 1, 2, uint256.Int{1}), address[:], true)
	}
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	require.Len(t, backend.deadlines, 2)
	require.Equal(t, backend.deadlines[0], backend.deadlines[1], "one slow batch must not get a new timeout for every frame")
}

func TestFramePoolStaleBatchPreservesPrecheckRejection(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	invalid := framePoolSlot(t, sender, 3, 2, uint256.Int{1})
	rejected := framePoolSlot(t, common.Address{2}, 1, 2, uint256.Int{1})
	pending := framePoolSlot(t, common.Address{3}, 1, 2, uint256.Int{1})
	ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
	ordinary.IDHash[0] = 4
	pool.frameRejections.Add(string(rejected.IDHash[:]), frameRejection{generation: pool.stateGeneration, reason: txpoolcfg.InvalidFrame})
	var txns TxnSlots
	for _, frame := range []*TxnSlot{invalid, rejected, pending} {
		address := frame.Txn.(*types.FrameTransaction).SenderAddress
		txns.Append(frame, address[:], true)
	}
	txns.Append(ordinary, sender[:], true)
	calls := 0
	pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
		calls++
		require.Equal(t, pending.Rlp, req.Transaction)
		require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	synctest.Test(t, func(t *testing.T) {
		reasons, err := pool.AddLocalTxns(t.Context(), txns)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{
			txpoolcfg.TipAboveFeeCap, txpoolcfg.FrameValidationUnavailable,
			txpoolcfg.FrameValidationUnavailable, txpoolcfg.Success,
		}, reasons)
		require.Equal(t, 1, calls, "precheck failures must not reach simulation")
		require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
	})
}

func TestFramePoolStaleBatchPreservesSenderConflict(t *testing.T) {
	for _, tc := range []struct {
		name        string
		key         uint64
		senderNonce uint64
		want        txpoolcfg.DiscardReason
	}{
		{name: "keyed", key: 2, want: txpoolcfg.InvalidFrame},
		{name: "legacy", want: txpoolcfg.InvalidFrame},
		{name: "consumed legacy nonce", senderNonce: 1, want: txpoolcfg.NonceTooLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, tc.senderNonce, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			blocker := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			var txns TxnSlots
			txns.Append(blocker, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			candidate := framePoolSlot(t, sender, 1, 2, uint256.Int{tc.key})
			otherSender := common.Address{2}
			pending := framePoolSlot(t, otherSender, 1, 2, uint256.Int{1})
			txns.Txns[0] = candidate
			txns.Append(pending, otherSender[:], true)
			calls := 0
			pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
				calls++
				require.Equal(t, pending.Rlp, req.Transaction)
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{2}, 0, otherSender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			synctest.Test(t, func(t *testing.T) {
				reasons, err := pool.AddLocalTxns(t.Context(), txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{tc.want, txpoolcfg.FrameValidationUnavailable}, reasons)
				require.Equal(t, 1, calls, "sender conflicts must not reach simulation")
				require.Contains(t, pool.byHash, string(blocker.IDHash[:]))
				require.NotContains(t, pool.byHash, string(candidate.IDHash[:]))
				rejection, ok := pool.frameRejections.Get(string(candidate.IDHash[:]))
				require.True(t, ok)
				require.Equal(t, tc.want, rejection.reason)
				require.True(t, rejection.applies(pool, pool.stateGeneration))
			})
		})
	}
}

func TestFramePoolHeadNotificationTimeout(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.err = status.Error(codes.Aborted, "frame validation head changed")
	var txns TxnSlots
	for _, address := range []common.Address{sender, {2}} {
		txns.Append(framePoolSlot(t, address, 1, 2, uint256.Int{1}), address[:], true)
	}
	ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
	ordinary.IDHash[0] = 2
	txns.Append(ordinary, sender[:], true)
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		reasons, err := pool.AddLocalTxns(t.Context(), txns)
		require.NoError(t, err)
		require.Equal(t, frameValidationTimeout, time.Since(started))
		require.Equal(t, []txpoolcfg.DiscardReason{
			txpoolcfg.FrameValidationUnavailable, txpoolcfg.FrameValidationUnavailable, txpoolcfg.Success,
		}, reasons)
		require.Len(t, backend.calls, 1, "no repeated backend calls while the pool head is unchanged")
		for _, txn := range txns.Txns[:2] {
			known, err := pool.IdHashKnown(nil, txn.IDHash[:])
			require.NoError(t, err)
			require.False(t, known, "timed-out frames must remain eligible for resubmission")
		}
		require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
	})
}

func TestFramePoolUnwindDoesNotWaitForHeadNotification(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.err = status.Error(codes.Aborted, "frame validation head changed")
	var unwound TxnSlots
	unwound.Append(framePoolSlot(t, sender, 1, 2), sender[:], true)
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		require.NoError(t, pool.OnNewBlock(t.Context(), newBlockChangeForAccounts(common.Hash{1}, 0, sender), unwound, TxnSlots{}, TxnSlots{}))
		require.Zero(t, time.Since(started), "state updates must return so the next notification can be processed")
		require.Len(t, backend.calls, 1)
	})
}

type frameValidationFuncBackend struct {
	remoteproto.ETHBACKENDClient
	validate func(context.Context, *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error)
}

func (b *frameValidationFuncBackend) FrameValidation(ctx context.Context, req *remoteproto.FrameValidationRequest, _ ...grpc.CallOption) (*remoteproto.FrameValidationReply, error) {
	return b.validate(ctx, req)
}

func TestFramePoolBoundsConcurrentValidation(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var calls atomic.Int32
	pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return backend.FrameValidation(ctx, req)
	}}
	var first TxnSlots
	first.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], true)
	done := make(chan error, 1)
	go func() {
		_, err := pool.AddLocalTxns(ctx, first)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first validation did not start")
	}
	limited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	var second TxnSlots
	other := common.Address{2}
	second.Append(framePoolSlot(t, other, 1, 2, uint256.Int{1}), other[:], true)
	_, err := pool.AddLocalTxns(limited, second)
	unblock()
	require.NoError(t, <-done)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 1, calls.Load(), "concurrent admission must not multiply backend simulation work")
}

func TestFramePoolRevalidationReleasesLock(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.onValidate = func() {
		require.True(t, pool.lock.TryLock(), "frame revalidation must not hold the pool lock")
		pool.lock.Unlock()
		pending, _, _ := pool.CountContent()
		require.Zero(t, pending, "a frame awaiting revalidation must not be selected")
	}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Len(t, backend.calls, 2)
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))
	require.Equal(t, 1, pool.pending.Len())
}

func TestFramePoolUnwindPooledBlobPreservesReservation(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.balance = common.Ether
	pool.pendingBlobFee.Store(1)

	blobTxn, err := types.DecodeWrappedTransaction(makeWrappedBlobTxnRlpWithCellProofs(t, uint256.NewInt(1), 1))
	require.NoError(t, err)
	blob := blobTxn.(*types.BlobTxWrapper)
	wrapper := &types.FrameTxWrapper{
		FrameTransaction: &types.FrameTransaction{
			ChainID: *uint256.NewInt(1), NonceKeys: []uint256.Int{{1}}, SenderAddress: sender,
			Frames:              []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}}},
			Fees:                types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2), BlobFeeCap: *uint256.NewInt(10)},
			BlobVersionedHashes: blob.Tx.BlobVersionedHashes,
		},
		WrapperVersion: 1, Blobs: blob.Blobs, Commitments: blob.Commitments, Proofs: blob.Proofs,
	}
	var encoded bytes.Buffer
	require.NoError(t, wrapper.MarshalBinaryWrapped(&encoded))
	frame := &TxnSlot{}
	parser := NewTxnParseContext(*uint256.NewInt(1))
	parser.WithSender(false)
	_, err = parser.ParseTransaction(encoded.Bytes(), 0, frame, nil, false, true, nil)
	require.NoError(t, err)

	failRevalidation := false
	pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
		if failRevalidation {
			failRevalidation = false
			return nil, context.DeadlineExceeded
		}
		reply, validationErr := backend.FrameValidation(ctx, req)
		if validationErr != nil {
			return nil, validationErr
		}
		cost := gointerfaces.ConvertH256ToUint256Int(reply.MaxCost)
		cost.AddUint64(cost, frame.Txn.GetBlobGas()*pool.pendingBlobFee.Load())
		reply.MaxCost = gointerfaces.ConvertUint256IntToH256(cost)
		return reply, nil
	}}
	ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
	ordinary.IDHash[0] = 2
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	txns.Append(ordinary, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	reservation := pool.frameReservations[sender]

	// Unwind must tolerate a blob transaction that is still pooled.
	var unwind TxnSlots
	unwind.Append(&TxnSlot{Txn: frame.Txn, IDHash: frame.IDHash}, sender[:], false)
	change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	change.PendingBlobFeePerGas = 2
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	failRevalidation = true
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, unwind, TxnSlots{}))
	require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	require.Equal(t, reservation, pool.frameReservations[sender])

	var mined TxnSlots
	mined.Append(frame, sender[:], false)
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{2}, 0, sender), TxnSlots{}, TxnSlots{}, mined))
	require.Empty(t, pool.frameReservations, "mining the only frame must release its reservation exactly")
	require.Equal(t, PendingSubPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
}

func TestFramePoolUnwindPreservesLocalFlag(t *testing.T) {
	for _, tc := range []struct {
		name  string
		local bool
	}{
		{name: "local", local: true},
		{name: "remote"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			var txns TxnSlots
			txns.Append(frame, sender[:], tc.local)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			txns.IsLocal[0] = false
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0, sender), TxnSlots{}, TxnSlots{}, txns))
			require.NotContains(t, pool.byHash, string(frame.IDHash[:]))

			var unwind TxnSlots
			unwind.Append(framePoolSlot(t, sender, 1, 2, uint256.Int{1}), sender[:], false)
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{2}, 0, sender), unwind, TxnSlots{}, TxnSlots{}))
			restored := pool.byHash[string(frame.IDHash[:])]
			require.NotNil(t, restored)
			require.Equal(t, tc.local, restored.subPool&IsLocal != 0)
		})
	}
}

func TestFramePoolUnwindRestoresNonceChain(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.queued.limit = 0
	pool.ethBackend.(*frameValidationBackend).balance = common.Ether
	frame := framePoolSlot(t, sender, 1, 2)
	blob := &TxnSlot{}
	encoded := makeWrappedBlobTxnRlpWithCellProofs(t, uint256.NewInt(1), 1)
	parser := NewTxnParseContext(*uint256.NewInt(1))
	parser.WithSender(false)
	_, err := parser.ParseTransaction(encoded, 0, blob, nil, false, true, nil)
	require.NoError(t, err)
	blob.Nonce = 1
	blob.Txn.(*types.BlobTx).Nonce = 1

	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	txns.Append(blob, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)

	mined := newBlockChangeForAccounts(common.Hash{1}, 2, sender)
	mined.ChangeBatch[0].BlockHeight = 1
	require.NoError(t, pool.OnNewBlock(ctx, mined, TxnSlots{}, TxnSlots{}, txns))
	require.Empty(t, pool.byHash)

	var unwind, unwindBlobs TxnSlots
	unwind.Append(frame, sender[:], false)
	unwindBlobs.Append(&TxnSlot{Txn: blob.Txn, Nonce: blob.Nonce, IDHash: blob.IDHash}, sender[:], false)
	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.NoError(t, pool.OnNewBlock(ctx, change, unwind, unwindBlobs, TxnSlots{}))

	for _, txn := range txns.Txns {
		restored := pool.byHash[string(txn.IDHash[:])]
		require.NotNil(t, restored, "transaction type %d with nonce %d was not restored", txn.TxType(), txn.Nonce)
		require.NotZero(t, restored.subPool&IsLocal)
		require.Equal(t, PendingSubPool, restored.currentSubPool)
	}
}

func TestFramePoolUnwindDoesNotReplaceBeforeValidation(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
	ordinary.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(ordinary, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	backend := pool.ethBackend.(*frameValidationBackend)
	backend.err = context.DeadlineExceeded
	frame := framePoolSlot(t, sender, 2, 4)
	var unwind TxnSlots
	unwind.Append(frame, sender[:], false)
	change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.NoError(t, pool.OnNewBlock(ctx, change, unwind, TxnSlots{}, TxnSlots{}))
	require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
	require.Equal(t, PendingSubPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
	require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
	require.Empty(t, backend.calls, "restoration cannot replace an admitted transaction without validation")
}

func TestFramePoolUnwindPreservesPooledNonceChain(t *testing.T) {
	for _, outcome := range []string{"valid", "invalid", "unavailable", "head mismatch", "backend error", "delegated", "nonce gap limit"} {
		t.Run(outcome, func(t *testing.T) {
			codeHash := accounts.EmptyCodeHash
			if outcome == "delegated" {
				codeHash = testDelegationCodeHash()
			}
			chainConfig := chain.AllProtocolChanges.Copy()
			chainConfig.BogotaTime = common.NewUint64(0)
			ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, codeHash, chainConfig)
			backend := &frameValidationBackend{payer: sender, balance: common.Ether, valid: true}
			pool.ethBackend = backend
			blockChange := func(hash common.Hash, nonce uint64) *remoteproto.StateChangeBatch {
				change := newBlockChangeForAccounts(hash, nonce, sender)
				account := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: codeHash}
				change.ChangeBatch[0].Changes[0].Data = accounts.SerialiseV3(&account)
				return change
			}
			frame := framePoolSlot(t, sender, 1, 2)
			var frameTxns TxnSlots
			frameTxns.Append(frame, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, frameTxns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			mined := blockChange(common.Hash{1}, 1)
			mined.ChangeBatch[0].BlockHeight = 1
			require.NoError(t, pool.OnNewBlock(ctx, mined, TxnSlots{}, TxnSlots{}, frameTxns))
			require.Empty(t, pool.byHash)

			var successors TxnSlots
			if outcome == "delegated" || outcome == "nonce gap limit" {
				count := uint64(1)
				if outcome == "nonce gap limit" {
					pool.cfg.MaxNonceGap = 1
					count = 2
				}
				for nonce := uint64(1); nonce <= count; nonce++ {
					txn := newTestTxnSlot(nonce, 0, 1, 2, 100_000)
					txn.IDHash[0] = byte(nonce)
					successors.Append(txn, sender[:], true)
				}
			} else {
				wrapped, parseErr := types.DecodeWrappedTransaction(makeWrappedBlobTxnRlpWithCellProofs(t, uint256.NewInt(1), 1))
				require.NoError(t, parseErr)
				wrapper := wrapped.(*types.BlobTxWrapper)
				wrapper.Tx.Nonce = 1
				var encoded bytes.Buffer
				require.NoError(t, wrapper.MarshalBinaryWrapped(&encoded))
				blob := &TxnSlot{}
				parser := NewTxnParseContext(*uint256.NewInt(1))
				parser.WithSender(false)
				_, parseErr = parser.ParseTransaction(encoded.Bytes(), 0, blob, nil, false, true, nil)
				require.NoError(t, parseErr)
				successors.Append(blob, sender[:], true)
			}
			reasons, err = pool.AddLocalTxns(ctx, successors)
			require.NoError(t, err)
			for _, reason := range reasons {
				require.Equal(t, txpoolcfg.Success, reason)
			}
			require.Equal(t, len(successors.Txns), pool.pending.Len())
			pool.queued.limit = 0

			backend.onValidate = func() {
				for _, txn := range successors.Txns {
					mt := pool.byHash[string(txn.IDHash[:])]
					require.NotNil(t, mt, "pooled successor must survive while its predecessor is being restored")
					require.Equal(t, QueuedSubPool, mt.currentSubPool)
				}
				var selected TxnsRlp
				_, err := pool.PeekBest(ctx, 10, &selected, 0)
				require.NoError(t, err)
				require.Empty(t, selected.Txns)
			}
			switch outcome {
			case "invalid":
				backend.valid = false
			case "unavailable":
				backend.err = context.DeadlineExceeded
			case "head mismatch":
				backend.err = status.Error(codes.Aborted, "frame validation head changed")
			case "backend error":
				backend.err = errors.New("validation backend unavailable")
			}
			change := blockChange(common.Hash{2}, 0)
			change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
			require.NoError(t, pool.OnNewBlock(ctx, change, frameTxns, TxnSlots{}, TxnSlots{}))
			if outcome == "invalid" {
				require.Empty(t, pool.byHash, "failed restoration must recheck the remaining nonce gap")
				return
			}
			if backend.err != nil {
				restored := pool.byHash[string(frame.IDHash[:])]
				require.NotNil(t, restored, "retryable validation failures must retain unwound frames")
				require.Zero(t, restored.currentSubPool)
				require.Nil(t, restored.TxnSlot.frameValidation)
				require.Empty(t, pool.frameReservations)
				for _, txn := range successors.Txns {
					mt := pool.byHash[string(txn.IDHash[:])]
					require.NotNil(t, mt)
					require.Equal(t, QueuedSubPool, mt.currentSubPool)
				}
				backend.err = nil
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{3}, 0), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			}
			restored := pool.byHash[string(frame.IDHash[:])]
			require.NotNil(t, restored)
			require.Equal(t, PendingSubPool, restored.currentSubPool)
			if outcome == "delegated" {
				require.Len(t, pool.byHash, 1)
				reason, ok := pool.discardReasonsLRU.Get(string(successors.Txns[0].IDHash[:]))
				require.True(t, ok)
				require.Equal(t, txpoolcfg.DelegatedTxnLimit, reason)
				return
			}
			for _, txn := range successors.Txns {
				mt := pool.byHash[string(txn.IDHash[:])]
				require.NotNil(t, mt)
				require.Equal(t, PendingSubPool, mt.currentSubPool)
				require.NotZero(t, mt.subPool&IsLocal)
				require.Equal(t, txn.BlobBundles, mt.TxnSlot.BlobBundles)
			}
		})
	}
}

func TestFramePoolUnwindKeepsUnrelatedQueueLimit(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	otherSender := common.Address{2}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender, otherSender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	pool.pending.limit, pool.baseFee.limit, pool.queued.limit = 2, 0, 1

	successor := newTestTxnSlot(1, 0, 1, 2, 21_000)
	successor.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(successor, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	backend.onValidate = func() {
		var unrelated TxnSlots
		for nonce := uint64(2); nonce <= 3; nonce++ {
			txn := newTestTxnSlot(nonce, 0, 1, 2, 21_000)
			txn.IDHash[0] = byte(nonce)
			unrelated.Append(txn, otherSender[:], false)
		}
		reasons, err := pool.AddLocalTxns(ctx, unrelated)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.QueuedPoolOverflow}, reasons)
		require.Contains(t, pool.byHash, string(successor.IDHash[:]))
		require.Equal(t, 2, pool.queued.Len(), "restoring chains must not consume the unrelated queue's allowance")
	}
	var unwound TxnSlots
	frame := framePoolSlot(t, sender, 1, 2)
	unwound.Append(frame, sender[:], false)
	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.NoError(t, pool.OnNewBlock(ctx, change, unwound, TxnSlots{}, TxnSlots{}))
	require.Equal(t, 2, pool.pending.Len())
	require.Equal(t, 1, pool.queued.Len())
}

func TestFramePoolUnwindKeepsTotalCapacity(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	pool.pending.limit, pool.baseFee.limit, pool.queued.limit = 2, 0, 0
	frame := framePoolSlot(t, sender, 1, 2)
	var frameTxns TxnSlots
	frameTxns.Append(frame, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, frameTxns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), TxnSlots{}, TxnSlots{}, frameTxns))

	successor := newTestTxnSlot(1, 0, 1, 2, 21_000)
	successor.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(successor, sender[:], true)
	reasons, err = pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	backend.onValidate = func() {
		var later TxnSlots
		for nonce := uint64(2); nonce <= 3; nonce++ {
			txn := newTestTxnSlot(nonce, 0, 1, 2, 21_000)
			txn.IDHash[0] = byte(nonce)
			later.Append(txn, sender[:], true)
		}
		reasons, err := pool.AddLocalTxns(ctx, later)
		require.NoError(t, err)
		require.Len(t, pool.byHash, 2, "restoring chains still count toward total capacity")
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.QueuedPoolOverflow, txpoolcfg.QueuedPoolOverflow}, reasons)
	}
	change := newBlockChangeForAccounts(common.Hash{2}, 0, sender)
	change.ChangeBatch[0].Direction = remoteproto.Direction_UNWIND
	require.NoError(t, pool.OnNewBlock(ctx, change, frameTxns, TxnSlots{}, TxnSlots{}))
	require.Equal(t, 2, pool.pending.Len())
	require.Zero(t, pool.queued.Len())
	for _, txn := range []*TxnSlot{frame, successor} {
		require.Contains(t, pool.byHash, string(txn.IDHash[:]))
	}
}

func TestFramePoolReloadRestoresNonceChain(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.chainID.SetUint64(1)
	pool.ethBackend.(*frameValidationBackend).balance = common.Ether
	frame := framePoolSlot(t, sender, 1, 2)

	wrapped, err := types.DecodeWrappedTransaction(makeWrappedBlobTxnRlpWithCellProofs(t, uint256.NewInt(1), 1))
	require.NoError(t, err)
	wrapper := wrapped.(*types.BlobTxWrapper)
	wrapper.Tx.Nonce = 1
	var encoded bytes.Buffer
	require.NoError(t, wrapper.MarshalBinaryWrapped(&encoded))
	blob := &TxnSlot{}
	parser := NewTxnParseContext(*uint256.NewInt(1))
	parser.WithSender(false)
	_, err = parser.ParseTransaction(encoded.Bytes(), 0, blob, nil, false, true, nil)
	require.NoError(t, err)

	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	txns.Append(blob, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	require.Equal(t, 2, pool.pending.Len())
	_, err = pool.flushNoFsync(ctx)
	require.NoError(t, err)

	restored, err := New(ctx, make(chan Announcements, 1), pool.poolDB, pool._chainDB,
		pool.cfg, pool._stateCache, pool.chainConfig, nil, nil, func() {}, nil,
		pool.ethBackend, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)
	restored.chainID.SetUint64(1)
	require.NoError(t, restored.start(ctx))
	for _, txn := range txns.Txns {
		mt := restored.byHash[string(txn.IDHash[:])]
		require.NotNil(t, mt, "transaction type %d with nonce %d was not restored", txn.TxType(), txn.Nonce)
		require.Equal(t, PendingSubPool, mt.currentSubPool)
		require.NotZero(t, mt.subPool&IsLocal)
		require.Equal(t, txn.BlobBundles, mt.TxnSlot.BlobBundles)
		require.Contains(t, restored.senderLastActivity, mt.TxnSlot.SenderID)
	}
}

func TestFramePoolReloadValidationBudgets(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.chainID.SetUint64(1)
	backend := pool.ethBackend.(*frameValidationBackend)
	var frames []*TxnSlot
	require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
		for _, address := range []common.Address{sender, {2}} {
			frame := framePoolSlot(t, address, 1, 2, uint256.Int{1})
			frames = append(frames, frame)
			if err := tx.Put(kv.PoolTransaction, frame.IDHash[:], append(address[:], frame.Rlp...)); err != nil {
				return err
			}
		}
		return nil
	}))

	synctest.Test(t, func(t *testing.T) {
		pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
			// Each validation fits its own budget, but together they exceed one budget.
			select {
			case <-time.After(frameValidationTimeout * 3 / 4):
				return backend.FrameValidation(ctx, req)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		require.NoError(t, pool.start(t.Context()))
		for _, frame := range frames {
			mt := pool.byHash[string(frame.IDHash[:])]
			require.NotNil(t, mt, "other restored frames must not consume this frame's validation budget")
			require.Equal(t, PendingSubPool, mt.currentSubPool)
		}
	})
}

func TestFramePoolRestoredTransactionsReleaseLock(t *testing.T) {
	for _, mode := range []string{"unwind", "reload"} {
		t.Run(mode, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			pool.chainID.SetUint64(1)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.onValidate = func() {
				require.True(t, pool.lock.TryLock(), "restored frame simulation must not hold the pool lock")
				pool.lock.Unlock()
			}
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			if mode == "unwind" {
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), txns, TxnSlots{}, TxnSlots{}))
			} else {
				require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.PoolTransaction, frame.IDHash[:], append(sender[:], frame.Rlp...))
				}))
				require.NoError(t, pool.start(ctx))
			}
			require.Contains(t, pool.byHash, string(frame.IDHash[:]))
			require.Len(t, backend.calls, 1)
		})
	}
}

func TestFramePoolStartupUsesExecutedHead(t *testing.T) {
	for _, mode := range []string{"new local transaction", "restored local transaction"} {
		t.Run(mode, func(t *testing.T) {
			chainConfig := chain.AllProtocolChanges.Copy()
			chainConfig.BogotaTime = common.NewUint64(0)
			ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
			pool.chainID.SetUint64(1)
			executedHead, headerHead := common.Hash{1}, common.Hash{2}
			require.NoError(t, coreDB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
				rawdb.WriteHeadBlockHash(tx, executedHead)
				return rawdb.WriteHeadHeaderHash(tx, headerHead)
			}))
			backend := &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
			pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
				if req.ExpectedBlockHash == nil || gointerfaces.ConvertH256ToHash(req.ExpectedBlockHash) != executedHead {
					return nil, status.Error(codes.Aborted, "frame validation head changed")
				}
				return backend.FrameValidation(ctx, req)
			}}
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			if mode == "restored local transaction" {
				require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
					if err := tx.Put(kv.PoolTransaction, frame.IDHash[:], append(sender[:], frame.Rlp...)); err != nil {
						return err
					}
					return tx.Put(kv.RecentLocalTransaction, make([]byte, 8), frame.IDHash[:])
				}))
			}
			require.NoError(t, pool.start(ctx))
			if mode == "new local transaction" {
				var txns TxnSlots
				txns.Append(frame, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			}
			require.Contains(t, pool.byHash, string(frame.IDHash[:]))
			require.Equal(t, 1, pool.pending.Len())
			require.NotZero(t, pool.byHash[string(frame.IDHash[:])].subPool&IsLocal)
		})
	}
}

func TestFramePoolReloadDeletesRejectedRows(t *testing.T) {
	for _, tc := range []struct {
		name          string
		valid         bool
		backendErr    error
		alreadyPooled bool
		wantKept      bool
	}{
		{name: "accepted", valid: true, wantKept: true},
		{name: "rejected by simulation"},
		{name: "backend unavailable", backendErr: errors.New("validation backend unavailable"), wantKept: true},
		{name: "head mismatch", backendErr: status.Error(codes.Aborted, "frame validation head changed"), wantKept: true},
		{name: "deadline", backendErr: context.DeadlineExceeded, wantKept: true},
		{name: "duplicate", valid: true, alreadyPooled: true, wantKept: true},
		{name: "rejected but already pooled", alreadyPooled: true, wantKept: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			pool.chainID.SetUint64(1)
			backend := pool.ethBackend.(*frameValidationBackend)
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			if tc.alreadyPooled {
				var txns TxnSlots
				txns.Append(frame, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				_, err = pool.flushNoFsync(ctx)
				require.NoError(t, err)
			} else {
				require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.PoolTransaction, frame.IDHash[:], append(sender[:], frame.Rlp...))
				}))
			}
			backend.valid, backend.err = tc.valid, tc.backendErr
			backend.calls = nil

			require.NoError(t, pool.start(ctx))
			if tc.alreadyPooled {
				require.Empty(t, backend.calls)
			} else {
				require.Len(t, backend.calls, 1)
			}
			require.Equal(t, tc.wantKept, pool.byHash[string(frame.IDHash[:])] != nil)
			if tc.wantKept && !tc.alreadyPooled {
				require.Contains(t, pool.senderLastActivity, pool.byHash[string(frame.IDHash[:])].TxnSlot.SenderID)
			}
			if tc.backendErr != nil {
				mt := pool.byHash[string(frame.IDHash[:])]
				require.Zero(t, mt.currentSubPool)
				require.Nil(t, mt.TxnSlot.frameValidation)
				require.Empty(t, pool.frameReservations)
			}
			_, err := pool.flushNoFsync(ctx)
			require.NoError(t, err)
			require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
				persisted, err := tx.Has(kv.PoolTransaction, frame.IDHash[:])
				require.NoError(t, err)
				require.Equal(t, tc.wantKept, persisted, "rejected restored rows must be removed on flush")
				return nil
			}))
			if tc.backendErr != nil {
				backend.err, backend.valid = nil, true
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0), TxnSlots{}, TxnSlots{}, TxnSlots{}))
				mt := pool.byHash[string(frame.IDHash[:])]
				require.NotNil(t, mt)
				require.Equal(t, PendingSubPool, mt.currentSubPool)
				require.NotNil(t, mt.TxnSlot.frameValidation)
				require.Equal(t, mt.TxnSlot.frameValidation.MaxCost, pool.frameReservations[sender].cost)
			}
		})
	}
}

func TestFramePoolReloadDeletesRejectedNonceSuccessor(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.chainID.SetUint64(1)
	frame := framePoolSlot(t, sender, 1, 2)
	ordinary := newTestTxnSlot(1, 0, 1, 2, 1)
	require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
		for _, txn := range []*TxnSlot{frame, ordinary} {
			var encoded bytes.Buffer
			if err := txn.Txn.MarshalBinary(&encoded); err != nil {
				return err
			}
			txn.IDHash = txn.Txn.Hash()
			if err := tx.Put(kv.PoolTransaction, txn.IDHash[:], append(sender[:], encoded.Bytes()...)); err != nil {
				return err
			}
		}
		return nil
	}))

	require.NoError(t, pool.start(ctx))
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))
	require.NotContains(t, pool.byHash, string(ordinary.IDHash[:]))
	_, err := pool.flushNoFsync(ctx)
	require.NoError(t, err)
	require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
		for _, txn := range []*TxnSlot{frame, ordinary} {
			persisted, err := tx.Has(kv.PoolTransaction, txn.IDHash[:])
			require.NoError(t, err)
			require.Equal(t, txn == frame, persisted, "transaction type %d", txn.TxType())
		}
		return nil
	}))
}

func TestFramePoolReloadErrorRetainsTransaction(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.chainID.SetUint64(1)
	backend := pool.ethBackend.(*frameValidationBackend)
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.PoolTransaction, frame.IDHash[:], append(sender[:], frame.Rlp...))
	}))
	backend.err = errors.New("validation backend unavailable")
	require.NoError(t, pool.start(ctx))
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))
	require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
		known, err := pool.IdHashKnown(tx, frame.IDHash[:])
		require.NoError(t, err)
		require.True(t, known, "retained frames must not need to be fetched again")
		return nil
	}))
	backend.err = nil
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, PendingSubPool, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	_, err := pool.flushNoFsync(ctx)
	require.NoError(t, err)
	require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
		persisted, err := tx.Has(kv.PoolTransaction, frame.IDHash[:])
		require.NoError(t, err)
		require.True(t, persisted)
		return nil
	}))
}

func TestFramePoolRevalidationQueuesNonceSuccessor(t *testing.T) {
	for _, funding := range []string{"sender", "sponsor"} {
		t.Run(funding, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			frameTx := &types.FrameTransaction{
				ChainID: *uint256.NewInt(1), NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}}},
				Fees:   types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)},
			}
			if funding == "sponsor" {
				backend.payer = common.Address{2}
				frameTx.Frames[0].Flags = types.FrameApproveExecution
				frameTx.Frames = append(frameTx.Frames, types.Frame{Mode: types.FrameVerify, Flags: types.FrameApprovePayment, Target: &backend.payer, Limits: mdgas.MdGas{Execution: 100}})
			}
			frame := &TxnSlot{Txn: frameTx}
			ordinary := newTestTxnSlot(1, 0, 1, 2, 21_000)
			var txns TxnSlots
			for _, txn := range []*TxnSlot{frame, ordinary} {
				var encoded bytes.Buffer
				require.NoError(t, txn.Txn.MarshalBinary(&encoded))
				txn.IDHash, txn.Rlp, txn.Size = txn.Txn.Hash(), encoded.Bytes(), uint32(encoded.Len())
				txns.Append(txn, sender[:], true)
			}
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
			require.Equal(t, 2, pool.pending.Len())

			checkParked := func() {
				require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
				var selected TxnsRlp
				_, err := pool.PeekBest(ctx, 10, &selected, 1)
				require.NoError(t, err)
				require.Empty(t, selected.Txns, "a parked frame cannot supply its successor's account nonce")
				require.Equal(t, QueuedSubPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
			}
			backend.onValidate = checkParked
			backend.err = context.DeadlineExceeded
			change := newBlockChangeForAccounts(common.Hash{1}, 0, backend.payer)
			change.ChangeBatch[0].BlockHeight = 1
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			checkParked()

			backend.err = nil
			change = newBlockChangeForAccounts(common.Hash{2}, 0)
			change.ChangeBatch[0].BlockHeight = 2
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.Len(t, backend.calls, 3)
			for _, txn := range txns.Txns {
				require.Equal(t, PendingSubPool, pool.byHash[string(txn.IDHash[:])].currentSubPool)
			}
		})
	}
}

func TestFramePoolRevalidationUnavailableRetriesOnNextBlock(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	ordinary := newTestTxnSlot(1, 0, 1, 2, 21_000)
	ordinary.IDHash[0] = 3
	var txns TxnSlots
	txns.Append(frame, frame.Txn.(*types.FrameTransaction).SenderAddress[:], true)
	txns.Append(ordinary, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success}, reasons)
	require.Equal(t, 1, pool.queued.Len())
	reservation := pool.frameReservations[sender]

	backend.err = context.DeadlineExceeded
	change := newBlockChangeForAccounts(common.Hash{1}, 1, sender)
	change.ChangeBatch[0].BlockHeight = 1
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, uint64(1), pool.lastSeenBlock.Load())
	require.Contains(t, pool.byHash, string(frame.IDHash[:]))
	require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	require.Equal(t, reservation, pool.frameReservations[sender])
	require.Equal(t, PendingSubPool, pool.byHash[string(ordinary.IDHash[:])].currentSubPool)
	require.Equal(t, 1, pool.pending.Len())

	backend.err = nil
	change = newBlockChangeForAccounts(common.Hash{2}, 1)
	change.ChangeBatch[0].BlockHeight = 2
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Len(t, backend.calls, 3, "parked transactions must be retried without another dependency change")
	require.Equal(t, PendingSubPool, pool.byHash[string(frame.IDHash[:])].currentSubPool)
	require.Equal(t, reservation, pool.frameReservations[sender])
	require.Equal(t, 2, pool.pending.Len())
}

func TestFramePoolParkedEvictionPriority(t *testing.T) {
	for _, nonceMode := range []string{"legacy", "keyed"} {
		for _, frameFee := range []uint64{2, 10} {
			t.Run(fmt.Sprintf("%s/frame fee %d", nonceMode, frameFee), func(t *testing.T) {
				ctx, pool, sender := keyedFramePoolEnv(t)
				backend := pool.ethBackend.(*frameValidationBackend)
				ordinarySender := common.Address{2}
				change := newBlockChangeForAccounts(common.Hash{1}, 0, ordinarySender)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				pool.pending.limit, pool.baseFee.limit, pool.queued.limit = 1, 0, 0

				key := uint256.Int{}
				if nonceMode == "keyed" {
					key.SetUint64(1)
				}
				frame := framePoolSlot(t, sender, 1, frameFee, key)
				var frames TxnSlots
				frames.Append(frame, sender[:], false)
				reasons, err := pool.addNewTxns(ctx, frames, nil)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

				backend.err = context.DeadlineExceeded
				change = newBlockChangeForAccounts(common.Hash{2}, 0, sender)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)

				ordinary := newTestTxnSlot(0, 0, 1, 5, 21_000)
				ordinary.IDHash[0] = 1
				var txns TxnSlots
				txns.Append(ordinary, ordinarySender[:], false)
				reasons, err = pool.addNewTxns(ctx, txns, nil)
				require.NoError(t, err)
				require.Len(t, pool.byHash, 1)
				if frameFee > 5 {
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.PendingPoolOverflow}, reasons)
					require.Contains(t, pool.byHash, string(frame.IDHash[:]))
					require.NotContains(t, pool.byHash, string(ordinary.IDHash[:]))
					require.Zero(t, pool.pending.Len())
					require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[sender].cost)
				} else {
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
					require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
					require.Contains(t, pool.byHash, string(ordinary.IDHash[:]))
					require.Equal(t, 1, pool.pending.Len())
					require.Empty(t, pool.frameReservations)
				}
			})
		}
	}
}

func TestFramePoolParkedCapacity(t *testing.T) {
	ctx, pool, payer := keyedFramePoolEnv(t)
	backend := pool.ethBackend.(*frameValidationBackend)
	pool.pending.limit, pool.baseFee.limit, pool.queued.limit = 1, 1, 1
	capacity := pool.pending.limit + pool.baseFee.limit + pool.queued.limit
	var frames []*TxnSlot
	for i := byte(1); i <= 8; i++ {
		backend.err = nil
		sender := common.Address{100, i}
		frame := framePoolSlot(t, sender, 1, uint64(i)+1, uint256.Int{1})
		frames = append(frames, frame)
		var txns TxnSlots
		txns.Append(frame, sender[:], false)
		reasons, err := pool.addNewTxns(ctx, txns, nil)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
		require.Equal(t, 1, pool.pending.Len())
		require.LessOrEqual(t, len(pool.byHash), capacity)

		backend.err = context.DeadlineExceeded
		change := newBlockChangeForAccounts(common.Hash{i}, 0, payer)
		change.ChangeBatch[0].BlockHeight = uint64(i)
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
		require.Zero(t, pool.pending.Len()+pool.baseFee.Len()+pool.queued.Len())
		require.LessOrEqual(t, len(pool.byHash), capacity)
	}

	var reserved uint256.Int
	for i, frame := range frames {
		_, retained := pool.byHash[string(frame.IDHash[:])]
		require.Equal(t, i >= len(frames)-capacity, retained, "retain the highest-fee frames")
		if retained {
			require.Contains(t, pool.all.senderIDFrameTxn, frame.SenderID)
			reserved.Add(&reserved, &frame.frameValidation.MaxCost)
		} else {
			require.NotContains(t, pool.all.senderIDFrameTxn, frame.SenderID)
			require.Zero(t, pool.all.count(frame.SenderID))
		}
	}
	require.Len(t, pool.byHash, capacity)
	require.Len(t, pool.all.senderIDFrameTxn, capacity)
	require.Equal(t, reserved, pool.frameReservations[payer].cost)
}

func TestFramePoolParkedCapacityWithOrdinaryOrphan(t *testing.T) {
	for _, name := range []string{"no parked frames", "one parked frame"} {
		t.Run(name, func(t *testing.T) {
			ctx, pool, payer := keyedFramePoolEnv(t)
			senders := []common.Address{{2}, {3}, {4}, {5}}
			change := newBlockChangeForAccounts(common.Hash{1}, 0, senders...)
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			pool.pending.limit, pool.baseFee.limit, pool.queued.limit = 1, 1, 1

			orphan := newTestTxnSlot(0, 0, 1, 2, 21_000)
			orphan.IDHash[0] = 1
			var initial TxnSlots
			initial.Append(orphan, senders[0][:], true)
			reasons, err := pool.AddLocalTxns(ctx, initial)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			// An ordinary txn left in the pool's indexes but outside every sub-pool.
			pool.pending.Remove(pool.byHash[string(orphan.IDHash[:])], "test", pool.logger)
			require.Zero(t, pool.pending.Len())
			require.Contains(t, pool.byHash, string(orphan.IDHash[:]))

			if name == "one parked frame" {
				frame := framePoolSlot(t, payer, 1, 2, uint256.Int{1})
				var frames TxnSlots
				frames.Append(frame, payer[:], true)
				reasons, err = pool.AddLocalTxns(ctx, frames)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				pool.ethBackend.(*frameValidationBackend).err = context.DeadlineExceeded
				change = newBlockChangeForAccounts(common.Hash{2}, 0, payer)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
			}

			var txns TxnSlots
			for i, txn := range []*TxnSlot{
				newTestTxnSlot(0, 0, 1, 2, 21_000),
				newTestTxnSlot(0, 0, 0, 0, 21_000),
				newTestTxnSlot(1, 0, 1, 2, 21_000),
			} {
				txn.IDHash[0] = byte(i + 2)
				txns.Append(txn, senders[i+1][:], true)
			}
			require.NotPanics(t, func() {
				reasons, err = pool.AddLocalTxns(ctx, txns)
			})
			require.NoError(t, err)
			require.Equal(t, 1, pool.pending.Len())
			if name == "one parked frame" {
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.BaseFeePoolOverflow, txpoolcfg.QueuedPoolOverflow}, reasons)
				require.Zero(t, pool.baseFee.Len()+pool.queued.Len())
				require.Len(t, pool.byHash, 3)
				require.Len(t, pool.all.senderIDFrameTxn, 1)
				require.Contains(t, pool.frameReservations, payer)
			} else {
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success, txpoolcfg.Success, txpoolcfg.Success}, reasons)
				require.Equal(t, 1, pool.baseFee.Len())
				require.Equal(t, 1, pool.queued.Len())
				require.Empty(t, pool.all.senderIDFrameTxn)
				require.Empty(t, pool.frameReservations)
			}
		})
	}
}

func TestFramePoolRevalidationAllowsResubmission(t *testing.T) {
	for _, failure := range []string{"invalid prefix", "payer balance"} {
		t.Run(failure, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			backend := pool.ethBackend.(*frameValidationBackend)
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			var txns TxnSlots
			txns.Append(frame, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			_, err = pool.flushNoFsync(ctx)
			require.NoError(t, err)

			switch failure {
			case "invalid prefix":
				backend.valid = false
			case "payer balance":
				backend.balance = 1
			}
			pool.lock.Lock()
			pending := pool.invalidateFrames(&remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{Changes: []*remoteproto.AccountChange{{
				Address: gointerfaces.ConvertAddressToH160(sender), Action: remoteproto.Action_STORAGE,
			}}}}})
			generation := pool.stateGeneration
			pool.lock.Unlock()
			require.NoError(t, pool.revalidateFrames(ctx, pending, generation, pool.stateBlockHash))
			require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
			require.Empty(t, pool.frameReservations)
			if failure == "invalid prefix" {
				require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			}
			require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
				persisted, readErr := tx.Has(kv.PoolTransaction, frame.IDHash[:])
				require.NoError(t, readErr)
				require.True(t, persisted)
				known, readErr := pool.IdHashKnown(tx, frame.IDHash[:])
				require.NoError(t, readErr)
				require.False(t, known)
				return nil
			}))

			backend.valid, backend.balance = true, 1_000_000
			txns.Txns[0] = framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			reasons, err = pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			require.Contains(t, pool.byHash, string(frame.IDHash[:]))
			require.Equal(t, frame.frameValidation.MaxCost, pool.frameReservations[sender].cost)
		})
	}
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
		{name: "manager account", action: remoteproto.Action_UPSERT},
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
			pending := pool.invalidateFrames(&remoteproto.StateChangeBatch{ChangeBatch: []*remoteproto.StateChange{{Direction: tc.direction, Changes: []*remoteproto.AccountChange{change}}}})
			generation := pool.stateGeneration
			pool.lock.Unlock()
			require.NoError(t, pool.revalidateFrames(ctx, pending, generation, pool.stateBlockHash))
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

func TestFramePoolExpiry(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		deadline               uint64
		blockTime              uint64
		baseFee                uint64
		elapsed                time.Duration
		gnosis                 bool
		expired                bool
		verifierChanged        bool
		verifierBalanceChanged bool
	}{
		{name: "before deadline", deadline: 100, blockTime: 87, baseFee: 1},
		{name: "at deadline", deadline: 100, blockTime: 88, baseFee: 1},
		{name: "waiting for base fee", deadline: 100, blockTime: 87, baseFee: 2},
		{name: "expired", deadline: 100, blockTime: 89, baseFee: 1, expired: true},
		{name: "expired below base fee", deadline: 100, blockTime: 89, baseFee: 2, expired: true},
		{name: "expired zero deadline", baseFee: 1, expired: true},
		{name: "missed slot", deadline: 100, blockTime: 88, elapsed: 101 * time.Second, baseFee: 1, expired: true},
		{name: "short slots at deadline", deadline: 100, blockTime: 95, baseFee: 1, gnosis: true},
		{name: "short slots past deadline", deadline: 100, blockTime: 96, baseFee: 1, gnosis: true, expired: true},
		{name: "verifier code changed", deadline: 100, blockTime: 87, baseFee: 1, verifierChanged: true},
		{name: "verifier balance changed", deadline: 100, blockTime: 87, baseFee: 1, verifierBalanceChanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, pool, sender := keyedFramePoolEnv(t)
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				now := uint64(time.Now().Unix())
				deadline := tc.deadline
				if deadline != 0 {
					deadline += now
				}
				if tc.gnosis {
					pool.chainConfig.Aura = &chain.AuRaConfig{}
				}
				initial := newBlockChangeForAccounts(common.Hash{1}, 0)
				initial.ChangeBatch[0].BlockTime = now
				initial.PendingBlockBaseFee = tc.baseFee
				require.NoError(t, pool.OnNewBlock(ctx, initial, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				tx := &types.FrameTransaction{
					NonceKeys: []uint256.Int{{1}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
					Frames: []types.Frame{
						{Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: binary.BigEndian.AppendUint64(nil, deadline), Limits: mdgas.MdGas{Execution: 5_000}},
						{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}},
					},
					Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(1)},
				}
				var encoded bytes.Buffer
				require.NoError(t, tx.MarshalBinary(&encoded))
				slot := &TxnSlot{Txn: tx, IDHash: tx.Hash(), Rlp: encoded.Bytes(), Size: uint32(encoded.Len())}
				var txns TxnSlots
				txns.Append(slot, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				backend := pool.ethBackend.(*frameValidationBackend)
				require.Len(t, backend.calls, 1)
				backend.calls = nil
				backend.valid = false
				wantPool := PendingSubPool
				if tc.baseFee > 1 {
					wantPool = BaseFeeSubPool
				}
				require.Equal(t, wantPool, pool.byHash[string(slot.IDHash[:])].currentSubPool)

				change := &remoteproto.StateChangeBatch{
					PendingBlockBaseFee: tc.baseFee, BlockGasLimit: 1_000_000,
					ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 1, BlockTime: now + tc.blockTime, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{2})}},
				}
				if tc.verifierChanged {
					change.ChangeBatch[0].Changes = []*remoteproto.AccountChange{{
						Address: gointerfaces.ConvertAddressToH160(types.FrameExpiryVerifier), Action: remoteproto.Action_CODE, Code: []byte{0xfe},
					}}
				}
				if tc.verifierBalanceChanged {
					acc := accounts.NewAccount()
					acc.Balance.SetUint64(1)
					change.ChangeBatch[0].Changes = []*remoteproto.AccountChange{{
						Address: gointerfaces.ConvertAddressToH160(types.FrameExpiryVerifier), Action: remoteproto.Action_UPSERT, Data: accounts.SerialiseV3(&acc),
					}}
				}
				time.Sleep(tc.elapsed)
				require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				if tc.verifierChanged {
					require.Equal(t, []common.Hash{slot.IDHash}, backend.calls)
				} else {
					require.Empty(t, backend.calls, "the fixed deadline must not require another simulation")
				}
				if tc.expired || tc.verifierChanged {
					require.NotContains(t, pool.byHash, string(slot.IDHash[:]))
					require.Empty(t, pool.frameReservations)
					require.Empty(t, pool.all.senderIDFrameTxn)
					require.Zero(t, pool.pending.Len()+pool.baseFee.Len()+pool.queued.Len())
					reason, ok := pool.discardReasonsLRU.Get(string(slot.IDHash[:]))
					require.True(t, ok)
					require.Equal(t, txpoolcfg.FrameStateChanged, reason)
					return
				}
				require.Contains(t, pool.byHash, string(slot.IDHash[:]))
				require.Equal(t, slot.frameValidation.MaxCost, pool.frameReservations[sender].cost)
				require.Equal(t, wantPool, pool.byHash[string(slot.IDHash[:])].currentSubPool)
			})
		})
	}
}

func TestFramePoolCreationNonceChanges(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		minNonce, maxNonce, nonce uint64
		revalidate                bool
	}{
		{"CREATE balance change", 1, 1, 1, false},
		{"CREATE nonce change", 1, 1, 2, true},
		{"CREATE2 nonce change", 0, math.MaxUint64 - 1, 2, false},
		{"CREATE2 nonce exhausted", 0, math.MaxUint64 - 1, math.MaxUint64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			factory := common.Address{2}
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.dependencies = []*typesproto.H160{gointerfaces.ConvertAddressToH160(factory)}
			backend.creationNonces = []*remoteproto.FrameCreationNonce{{Address: gointerfaces.ConvertAddressToH160(factory), MinNonce: tc.minNonce, MaxNonce: tc.maxNonce}}
			tx := &types.FrameTransaction{
				NonceKeys: []uint256.Int{{}}, ChainID: *uint256.NewInt(1), SenderAddress: sender,
				Frames: []types.Frame{
					{Mode: types.FrameDefault, Target: &factory, Limits: mdgas.MdGas{Execution: 10_000, State: 400_000}},
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 100}},
				},
				Fees: types.FrameFees{TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(1)},
			}
			var encoded bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&encoded))
			slot := &TxnSlot{Txn: tx, IDHash: tx.Hash(), Rlp: encoded.Bytes(), Size: uint32(encoded.Len())}
			var txns TxnSlots
			txns.Append(slot, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txns)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			backend.calls, backend.valid = nil, false
			acc := accounts.NewAccount()
			acc.Nonce = tc.nonce
			acc.Balance.SetUint64(2)
			change := newBlockChangeForAccounts(common.Hash{1}, 0)
			change.ChangeBatch[0].Changes = []*remoteproto.AccountChange{{
				Address: gointerfaces.ConvertAddressToH160(factory), Action: remoteproto.Action_UPSERT, Data: accounts.SerialiseV3(&acc),
			}}
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

func TestFramePoolReservationsAndReplacement(t *testing.T) {
	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.BogotaTime = common.NewUint64(0)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
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
	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.BogotaTime = common.NewUint64(0)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
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
	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.BogotaTime = common.NewUint64(0)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
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
	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.BogotaTime = common.NewUint64(0)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, testDelegationCodeHash(), chainConfig)
	pool.ethBackend = &frameValidationBackend{payer: sender, balance: common.Ether, valid: true}
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
	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.BogotaTime = common.NewUint64(0)
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash, chainConfig)
	pool.ethBackend = &frameValidationBackend{payer: sender, balance: 1_000_000, valid: true}
	return ctx, pool, sender
}
