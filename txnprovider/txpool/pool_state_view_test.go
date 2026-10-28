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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
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
					require.NoError(t, pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{
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
				require.NoError(t, pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{
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
