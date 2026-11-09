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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestPoolReopensStateAfterNewBlock(t *testing.T) {
	for _, mode := range []string{"local", "remote", "reload"} {
		for _, height := range []uint64{0, 1} {
			t.Run(fmt.Sprintf("%s/height=%d", mode, height), func(t *testing.T) {
				ctx, pool, poolDB, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
				pool.started.Store(mode != "reload")
				var txns TxnSlots
				for nonce := range uint64(3) {
					txn := newTestTxnSlot(nonce, 0, 1, 2, 100_000)
					txn.IDHash = txn.Txn.Hash()
					txns.Append(txn, sender[:], mode == "local")
				}
				if mode == "reload" {
					require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
						for _, txn := range txns.Txns {
							var encoded bytes.Buffer
							if err := txn.Txn.MarshalBinary(&encoded); err != nil {
								return err
							}
							if err := tx.Put(kv.PoolTransaction, txn.IDHash[:], append(sender[:], encoded.Bytes()...)); err != nil {
								return err
							}
						}
						return nil
					}))
				}

				probe := &stateViewProbe{Cache: pool._stateCache}
				pool._stateCache = probe
				probe.onView = func() {
					require.True(t, pool.lock.TryLock(), "opening state must allow block updates")
					pool.lock.Unlock()
					account := accounts.Account{
						Nonce:    1,
						Balance:  *uint256.NewInt(common.Ether),
						CodeHash: testDelegationCodeHash(),
					}
					encoded := accounts.SerialiseV3(&account)
					writeTestSenderState(t, ctx, coreDB, pool.logger, sender, encoded, 1)
					require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
						return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, 1))
					}))
					require.NoError(t, applyTestBlock(ctx, pool, &remoteproto.StateChangeBatch{
						StateVersionId:      1,
						PendingBlockBaseFee: 1,
						BlockGasLimit:       1_000_000,
						ChangeBatch: []*remoteproto.StateChange{{
							BlockHeight: height,
							BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{1}),
							Changes: []*remoteproto.AccountChange{{
								Action:  remoteproto.Action_UPSERT,
								Address: gointerfaces.ConvertAddressToH160(sender),
								Data:    encoded,
							}},
						}},
					}, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				}

				switch mode {
				case "local":
					reasons, err := pool.AddLocalTxns(ctx, txns)
					require.NoError(t, err)
					require.Equal(t, []txpoolcfg.DiscardReason{
						txpoolcfg.NonceTooLow, txpoolcfg.Success, txpoolcfg.DelegatedNonceGap,
					}, reasons)
				case "remote":
					pool.AddRemoteTxns(ctx, txns, nil, nil)
					require.NoError(t, pool.processRemoteTxns(ctx))
				case "reload":
					require.NoError(t, pool.start(ctx))
				}
				require.Nil(t, probe.onView, "the new block must overtake the old state view")
				checkPool := func() {
					pending, baseFee, queued := pool.CountContent()
					require.Equal(t, 1, pending)
					require.Zero(t, baseFee+queued)
					require.NotContains(t, pool.byHash, string(txns.Txns[0].IDHash[:]))
					require.Contains(t, pool.byHash, string(txns.Txns[1].IDHash[:]))
					require.NotContains(t, pool.byHash, string(txns.Txns[2].IDHash[:]))
				}
				checkPool()
				require.NoError(t, applyTestBlock(ctx, pool, &remoteproto.StateChangeBatch{
					StateVersionId:      1,
					PendingBlockBaseFee: 1,
					BlockGasLimit:       1_000_000,
					ChangeBatch: []*remoteproto.StateChange{{
						BlockHeight: height + 1,
						BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{2}),
					}},
				}, TxnSlots{}, TxnSlots{}, TxnSlots{}))
				checkPool()
			})
		}
	}
}

func TestPoolExecutionViewRecovery(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			ctx, pool, _, coreDB := newTestPool(t)
			pool._stateCache = kvcache.NewLatestBatchCache()
			if remote {
				pool._chainDB = remoteTestDB(t, coreDB, pool.logger)
			}
			sender := common.Address{1}
			account := accounts.Account{Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
			writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&account), 1)
			previous := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(common.Hash{})}
			require.NoError(t, coreDB.View(ctx, func(tx kv.Tx) (err error) {
				previous.StateVersionId, err = rawdb.GetStateVersion(tx)
				return err
			}))
			batch := &remoteproto.StateChangeBatch{TargetView: previous, StateChangesIncomplete: true, BlockGasLimit: 1_000_000, PendingBlockBaseFee: 1}
			require.NoError(t, pool.OnNewBlock(ctx, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			add := func(nonce uint64) *TxnSlot {
				txn := newTestTxnSlot(nonce, 0, 1, 2, 100_000)
				txn.IDHash = txn.Txn.Hash()
				var slots TxnSlots
				slots.Append(txn, sender[:], true)
				reasons, err := pool.AddLocalTxns(ctx, slots)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				return txn
			}
			consumed := add(0)
			target := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(common.Hash{1}), BlockHeight: 1, StateVersionId: previous.StateVersionId + 1}
			account.Nonce = 1
			batch = &remoteproto.StateChangeBatch{
				PreviousView: previous, TargetView: target, BlockGasLimit: 1_000_000, PendingBlockBaseFee: 1,
				ChangeBatch: []*remoteproto.StateChange{{Changes: []*remoteproto.AccountChange{{Address: gointerfaces.ConvertAddressToH160(sender), Action: remoteproto.Action_UPSERT, Data: accounts.SerialiseV3(&account)}}}},
			}
			require.NoError(t, pool.OnNewBlock(ctx, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.NotContains(t, pool.byHash, string(consumed.IDHash[:]), "complete changes must work before commit")
			writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&account), 2)
			commitView := func(view *remoteproto.ExecutionView) {
				require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
					rawdb.WriteHeadBlockHash(tx, common.Hash(gointerfaces.ConvertH256ToHash(view.BlockHash)))
					return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, view.StateVersionId))
				}))
			}
			commitView(target)
			consumed = add(1)
			reset := proto.Clone(batch).(*remoteproto.StateChangeBatch)
			reset.PreviousView = target
			reset.TargetView.StateVersionId++
			reset.StateChangesIncomplete = true
			require.ErrorIs(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}), kvcache.ErrExecutionViewUnavailable)
			selected, err := pool.ProvideTxns(ctx)
			require.NoError(t, err)
			require.Empty(t, selected)
			account.Nonce = 2
			writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&account), 3)
			commitView(reset.TargetView)
			require.NoError(t, pool.OnNewBlock(ctx, reset, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			require.NotContains(t, pool.byHash, string(consumed.IDHash[:]), "reset must ignore recorded account values")
		})
	}
}

func TestResetRefreshesOrdinarySenders(t *testing.T) {
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	txn := newTestTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash = txn.Txn.Hash()
	var txns TxnSlots
	txns.Append(txn, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&accounts.Account{
		Nonce: 1, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
	}), 1)
	require.NoError(t, applyTestBlock(ctx, pool, &remoteproto.StateChangeBatch{
		StateChangesIncomplete: true,
		PendingBlockBaseFee:    1, BlockGasLimit: 1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 1}},
	}, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.NotContains(t, pool.byHash, string(txn.IDHash[:]))
}

func TestPoolResetWaitsForTarget(t *testing.T) {
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	txn := newTestTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash = txn.Txn.Hash()
	var txns TxnSlots
	txns.Append(txn, sender[:], true)
	_, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	target := &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(common.Hash{1}), BlockHeight: 1, StateVersionId: 7}
	batch := &remoteproto.StateChangeBatch{
		TargetView: target, StateVersionId: 7, StateChangesIncomplete: true,
		PendingBlockBaseFee: 1, BlockGasLimit: 1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 9}},
	}
	require.Error(t, applyTestBlock(ctx, pool, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	selected, err := pool.ProvideTxns(ctx)
	require.NoError(t, err)
	require.Empty(t, selected, "an unsuccessful refresh must stop selection")
	writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&accounts.Account{
		Nonce: 1, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
	}), 1)
	require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
		rawdb.WriteHeadBlockHash(tx, common.Hash{1})
		return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, 7))
	}))
	require.NoError(t, applyTestBlock(ctx, pool, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, uint64(1), pool.lastSeenBlock.Load())
	require.NotContains(t, pool.byHash, string(txn.IDHash[:]))
}

func TestPoolResetRestoresOrphansAcrossNotificationGap(t *testing.T) {
	ctx, pool, _, coreDB := newTestPool(t)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{2}, uint256.NewInt(0), 21_000, uint256.NewInt(2), nil), *types.LatestSignerForChainID(&pool.chainID), key)
	require.NoError(t, err)
	genesis := types.NewBlock(&types.Header{}, nil, nil, nil, nil, nil)
	old := types.NewBlock(&types.Header{Number: *uint256.NewInt(1), ParentHash: genesis.Hash(), Extra: []byte{1}}, []types.Transaction{txn}, nil, nil, nil, nil)
	head := types.NewBlock(&types.Header{Number: *uint256.NewInt(1), ParentHash: genesis.Hash(), Extra: []byte{2}}, nil, nil, nil, nil, nil)
	pool.appliedView = &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(old.Hash()), BlockHeight: 1, StateVersionId: 1}
	writeTestSenderState(t, ctx, coreDB, pool.logger, sender, accounts.SerialiseV3(&accounts.Account{
		Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
	}), 1)
	require.NoError(t, coreDB.Update(ctx, func(tx kv.RwTx) error {
		for _, block := range []*types.Block{genesis, old, head} {
			if err := rawdb.WriteBlock(tx, block); err != nil {
				return err
			}
		}
		rawdb.WriteHeadBlockHash(tx, head.Hash())
		return tx.Put(kv.Sequence, kv.PlainStateVersion, binary.BigEndian.AppendUint64(nil, 2))
	}))
	batch := &remoteproto.StateChangeBatch{
		TargetView:     &remoteproto.ExecutionView{BlockHash: gointerfaces.ConvertHashToH256(head.Hash()), BlockHeight: 1, StateVersionId: 2},
		StateVersionId: 2, StateChangesIncomplete: true, PendingBlockBaseFee: 1, BlockGasLimit: 1_000_000,
	}
	cache := &unavailableAccountCache{Cache: pool._stateCache, unavailable: true}
	pool._stateCache = cache
	require.Error(t, applyTestBlock(ctx, pool, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Equal(t, uint64(1), pool.appliedView.StateVersionId, "delivery is not acknowledgement")
	cache.unavailable = false
	require.NoError(t, applyTestBlock(ctx, pool, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	hash := txn.Hash()
	require.Contains(t, pool.byHash, string(hash[:]))
	require.NoError(t, applyTestBlock(ctx, pool, batch, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.Len(t, pool.byHash, 1, "retry must not replay membership changes")
}

type unavailableAccountCache struct {
	kvcache.Cache
	unavailable bool
}

func (c *unavailableAccountCache) View(ctx context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	view, err := c.Cache.View(ctx, tx)
	return unavailableAccountView{view, c.unavailable}, err
}

type unavailableAccountView struct {
	kvcache.CacheView
	unavailable bool
}

func (v unavailableAccountView) Get(key []byte) ([]byte, error) {
	if v.unavailable {
		return nil, errors.New("account read unavailable")
	}
	return v.CacheView.Get(key)
}
