/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkName(b *testing.B) {
	txs := make([]*metaTx, 10_000)
	p := NewSubPool(BaseFeeSubPool, 1024)
	for i := 0; i < len(txs); i++ {
		txs[i] = &metaTx{Tx: &TxSlot{}}
	}
	for i := 0; i < len(txs); i++ {
		p.Add(txs[i])
	}
	p.EnforceInvariants()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txs[0].timestamp = 1
		heap.Fix(p.best, txs[0].bestIndex)
		heap.Fix(p.worst, txs[0].worstIndex)
	}
}

func BenchmarkName2(b *testing.B) {

	var (
		a = rand.Uint64()
		c = rand.Uint64()
		d = rand.Uint64()
	)
	b.ResetTimer()
	var min1 uint64
	var min2 uint64
	var r uint64

	for i := 0; i < b.N; i++ {
		min1 = min(min1, a)
		min2 = min(min2, c)
		if d <= min1 {
			r = min(min1-d, min2)
		} else {
			r = 0
		}
		//
		//// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than
		//// baseFee of the currently pending block. Set to 0 otherwise.
		//mt.subPool &^= EnoughFeeCapBlock
		//if mt.Tx.feeCap >= pendingBaseFee {
		//	mt.subPool |= EnoughFeeCapBlock
		//}
	}
	_ = r
}

func TestNonceFromAddress(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Hashes, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var txID uint64
	_ = coreDB.View(ctx, func(tx kv.Tx) error {
		txID = tx.ViewID()
		return nil
	})
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		DatabaseViewID:      txID,
		PendingBlockBaseFee: pendingBaseFee,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxSlots{}, TxSlots{}, tx)
	assert.NoError(err)

	{
		var txSlots TxSlots
		txSlot1 := &TxSlot{
			tip:    300000,
			feeCap: 300000,
			gas:    100000,
			nonce:  3,
		}
		txSlot1.IdHash[0] = 1
		txSlots.Append(txSlot1, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}

	{
		txSlots := TxSlots{}
		txSlot2 := &TxSlot{
			tip:    300000,
			feeCap: 300000,
			gas:    100000,
			nonce:  4,
		}
		txSlot2.IdHash[0] = 2
		txSlot3 := &TxSlot{
			tip:    300000,
			feeCap: 300000,
			gas:    100000,
			nonce:  6,
		}
		txSlot3.IdHash[0] = 3
		txSlots.Append(txSlot2, addr[:], true)
		txSlots.Append(txSlot3, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(6), nonce)
	}
	// test too expencive tx
	{
		var txSlots TxSlots
		txSlot1 := &TxSlot{
			tip:    9 * common.Ether,
			feeCap: 300000,
			gas:    100000,
			nonce:  3,
		}
		txSlot1.IdHash[0] = 4
		txSlots.Append(txSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(InsufficientFunds, reason, reason.String())
		}
	}

	// test too low nonce
	{
		var txSlots TxSlots
		txSlot1 := &TxSlot{
			tip:    300000,
			feeCap: 300000,
			gas:    100000,
			nonce:  1,
		}
		txSlot1.IdHash[0] = 5
		txSlots.Append(txSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(NonceTooLow, reason, reason.String())
		}
	}
}

func TestReplaceWithHigherFee(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Hashes, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var txID uint64
	_ = coreDB.View(ctx, func(tx kv.Tx) error {
		txID = tx.ViewID()
		return nil
	})
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		DatabaseViewID:      txID,
		PendingBlockBaseFee: pendingBaseFee,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxSlots{}, TxSlots{}, tx)
	assert.NoError(err)

	{
		var txSlots TxSlots
		txSlot := &TxSlot{
			tip:    300000,
			feeCap: 300000,
			gas:    100000,
			nonce:  3,
		}
		txSlot.IdHash[0] = 1
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	// Bumped only feeCap, transaction not accepted
	{
		txSlots := TxSlots{}
		txSlot := &TxSlot{
			tip:    300000,
			feeCap: 3000000,
			gas:    100000,
			nonce:  3,
		}
		txSlot.IdHash[0] = 2
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(NotReplaced, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(3), nonce)
	}
	// Bumped only tip, transaction not accepted
	{
		txSlots := TxSlots{}
		txSlot := &TxSlot{
			tip:    3000000,
			feeCap: 300000,
			gas:    100000,
			nonce:  3,
		}
		txSlot.IdHash[0] = 3
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(NotReplaced, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(3), nonce)
	}
	// Bumped both tip and feeCap by 10%, tx accepted
	{
		txSlots := TxSlots{}
		txSlot := &TxSlot{
			tip:    330000,
			feeCap: 330000,
			gas:    100000,
			nonce:  3,
		}
		txSlot.IdHash[0] = 4
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(3), nonce)
	}
}

func TestReverseNonces(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Hashes, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var txID uint64
	_ = coreDB.View(ctx, func(tx kv.Tx) error {
		txID = tx.ViewID()
		return nil
	})
	pendingBaseFee := uint64(1_000_000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		DatabaseViewID:      txID,
		PendingBlockBaseFee: pendingBaseFee,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxSlots{}, TxSlots{}, tx)
	assert.NoError(err)
	// 1. Send high fee transaction with nonce gap
	{
		var txSlots TxSlots
		txSlot := &TxSlot{
			tip:    500_000,
			feeCap: 3_000_000,
			gas:    100000,
			nonce:  3,
		}
		txSlot.IdHash[0] = 1
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	fmt.Printf("AFTER TX 1\n")
	select {
	case hashes := <-ch:
		for i := 0; i < hashes.Len(); i++ {
			fmt.Printf("propagated hash %x\n", hashes.At(i))
		}
	default:

	}
	// 2. Send low fee (below base fee) transaction without nonce gap
	{
		var txSlots TxSlots
		txSlot := &TxSlot{
			tip:    500_000,
			feeCap: 500_000,
			gas:    100000,
			nonce:  2,
		}
		txSlot.IdHash[0] = 2
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	fmt.Printf("AFTER TX 2\n")
	select {
	case hashes := <-ch:
		for i := 0; i < hashes.Len(); i++ {
			fmt.Printf("propagated hash %x\n", hashes.At(i))
		}
	default:

	}

	{
		var txSlots TxSlots
		txSlot := &TxSlot{
			tip:    600_000,
			feeCap: 3_000_000,
			gas:    100000,
			nonce:  2,
		}
		txSlot.IdHash[0] = 3
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	fmt.Printf("AFTER TX 3\n")
	select {
	case hashes := <-ch:
		for i := 0; i < hashes.Len(); i++ {
			fmt.Printf("propagated hash %x\n", hashes.At(i))
		}
	default:

	}
}
