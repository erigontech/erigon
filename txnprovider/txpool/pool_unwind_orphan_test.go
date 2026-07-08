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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// A deep unwind can re-inject two different transactions that share the same
// (sender, nonce). The second is rejected as NotReplaced; its rejection path must
// not delete from p.all, which is keyed on (sender, nonce) only — deleting there
// removes the resident first transaction, orphaning it in the queued sub-pool with
// no p.all backing. That later crashes senders.info once the now-empty sender is
// garbage-collected on flush.
func TestUnwindDuplicateSenderNonceKeepsQueuedConsistent(t *testing.T) {
	require := require.New(t)
	ch := make(chan Announcements, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)

	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(1 * common.Ether), CodeHash: accounts3.EmptyCodeHash, Incarnation: 1}
	accV := accounts3.SerialiseV3(&acc)
	h0 := gointerfaces.ConvertHashToH256([32]byte{})
	initChange := &remoteproto.StateChangeBatch{
		StateVersionId:      0,
		PendingBlockBaseFee: 200000,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 0,
			BlockHash:   h0,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    accV,
			}},
		}},
	}
	require.NoError(pool.OnNewBlock(ctx, initChange, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	// Two distinct txns (different hashes) with the same (sender, nonce=1). nonce 1
	// against on-chain nonce 0 is nonce-gapped, so both classify to the queued pool.
	txnA := newTestTxnSlot(1, 0, 300000, 300000, 100000)
	txnA.IDHash[0] = 0xAA
	txnB := newTestTxnSlot(1, 0, 300000, 300000, 100000)
	txnB.IDHash[0] = 0xBB
	var unwind TxnSlots
	unwind.Append(txnA, addr[:], false)
	unwind.Append(txnB, addr[:], false)
	h1 := gointerfaces.ConvertHashToH256([32]byte{1})
	block1 := &remoteproto.StateChangeBatch{
		StateVersionId:      1,
		PendingBlockBaseFee: 200000,
		BlockGasLimit:       1000000,
		ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 1, BlockHash: h1}},
	}
	require.NoError(pool.OnNewBlock(ctx, block1, unwind, TxnSlots{}, TxnSlots{}))

	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, mt := range pool.queued.best.ms {
		sid := mt.TxnSlot.SenderID
		require.True(pool.all.hasTxns(sid), "queued txn orphaned: sender %d absent from p.all", sid)
		_, ok := pool.senders.senderID2Addr[sid]
		require.True(ok, "queued txn's sender %d absent from senderID2Addr", sid)
	}
}
