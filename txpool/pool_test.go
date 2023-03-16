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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/types"
)

func TestNonceFromAddress(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan types.Announcements, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionID:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(err)

	{
		var txSlots types.TxSlots
		txSlot1 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot1.IDHash[0] = 1
		txSlots.Append(txSlot1, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}

	{
		txSlots := types.TxSlots{}
		txSlot2 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  4,
		}
		txSlot2.IDHash[0] = 2
		txSlot3 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  6,
		}
		txSlot3.IDHash[0] = 3
		txSlots.Append(txSlot2, addr[:], true)
		txSlots.Append(txSlot3, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
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
		var txSlots types.TxSlots
		txSlot1 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(9 * common.Ether),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot1.IDHash[0] = 4
		txSlots.Append(txSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(InsufficientFunds, reason, reason.String())
		}
	}

	// test too low nonce
	{
		var txSlots types.TxSlots
		txSlot1 := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  1,
		}
		txSlot1.IDHash[0] = 5
		txSlots.Append(txSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(NonceTooLow, reason, reason.String())
		}
	}
}

func TestReplaceWithHigherFee(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan types.Announcements, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionID:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(err)

	{
		var txSlots types.TxSlots
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot.IDHash[0] = 1
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	// Bumped only feeCap, transaction not accepted
	{
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot.IDHash[0] = 2
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
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
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(3000000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot.IDHash[0] = 3
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
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
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(330000),
			FeeCap: *uint256.NewInt(330000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot.IDHash[0] = 4
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
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
	ch := make(chan types.Announcements, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(1_000_000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionID:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(err)
	// 1. Send high fee transaction with nonce gap
	{
		var txSlots types.TxSlots
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(500_000),
			FeeCap: *uint256.NewInt(3_000_000),
			Gas:    100000,
			Nonce:  3,
		}
		txSlot.IDHash[0] = 1
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	fmt.Printf("AFTER TX 1\n")
	select {
	case annoucements := <-ch:
		for i := 0; i < annoucements.Len(); i++ {
			_, _, hash := annoucements.At(i)
			fmt.Printf("propagated hash %x\n", hash)
		}
	default:

	}
	// 2. Send low fee (below base fee) transaction without nonce gap
	{
		var txSlots types.TxSlots
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(500_000),
			FeeCap: *uint256.NewInt(500_000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 2
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	fmt.Printf("AFTER TX 2\n")
	select {
	case annoucements := <-ch:
		for i := 0; i < annoucements.Len(); i++ {
			_, _, hash := annoucements.At(i)
			fmt.Printf("propagated hash %x\n", hash)
		}
	default:

	}

	{
		var txSlots types.TxSlots
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(600_000),
			FeeCap: *uint256.NewInt(3_000_000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 3
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	fmt.Printf("AFTER TX 3\n")
	select {
	case annoucements := <-ch:
		for i := 0; i < annoucements.Len(); i++ {
			_, _, hash := annoucements.At(i)
			fmt.Printf("propagated hash %x\n", hash)
		}
	default:

	}
}

// When local transaction is send to the pool, but it cannot replace existing transaction,
// the existing transaction gets "poked" and is getting re-broadcasted
// this is a workaround for cases when transactions are getting stuck for strage reasons
// even though logs show they are broadcast
func TestTxPoke(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan types.Announcements, 100)
	db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

	cfg := DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil)
	assert.NoError(err)
	require.True(pool != nil)
	ctx := context.Background()
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionID:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(1 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(1 * common.Ether), v)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
	assert.NoError(err)

	var idHash types.Hashes
	{
		var txSlots types.TxSlots
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 1
		idHash = append(idHash, txSlot.IDHash[:]...)
		txSlots.Append(txSlot, addr[:], true)

		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(Success, reason, reason.String())
		}
	}
	var promoted types.Announcements
	select {
	case promoted = <-ch:
		if !bytes.Equal(idHash, promoted.DedupHashes()) {
			t.Errorf("expected promoted %x, got %x", idHash, promoted)
		}
	default:
		t.Errorf("expected promotion")
	}
	// Send the same transaction, not accepted
	{
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 1
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(DuplicateHash, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(2), nonce)
	}
	// Even though transaction not replaced, it gets poked
	select {
	case promoted = <-ch:
		if !bytes.Equal(idHash, promoted.Hashes()) {
			t.Errorf("expected promoted %x, got %x", idHash, promoted)
		}
	default:
		t.Errorf("expected promotion")
	}
	// Send different transaction, but only with tip bumped
	{
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(3000000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 2
		txSlots.Append(txSlot, addr[:], true)
		reasons, err := pool.AddLocalTxs(ctx, txSlots, tx)
		assert.NoError(err)
		for _, reason := range reasons {
			assert.Equal(NotReplaced, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(2), nonce)
	}
	// Even though transaction not replaced, it gets poked
	select {
	case promoted = <-ch:
		if !bytes.Equal(idHash, promoted.Hashes()) {
			t.Errorf("expected promoted %x, got %x", idHash, promoted)
		}
	default:
		t.Errorf("expected promotion")
	}

	// Send the same transaction, but as remote
	{
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 1
		txSlots.Append(txSlot, addr[:], true)
		pool.AddRemoteTxs(ctx, txSlots)
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(2), nonce)
	}
	// Remote transactions do not cause pokes
	select {
	case <-ch:
		t.Errorf("remote transactions should not cause re-broadcast")
	default:
	}
	// Send different transaction, but only with tip bumped, as a remote
	{
		txSlots := types.TxSlots{}
		txSlot := &types.TxSlot{
			Tip:    *uint256.NewInt(3000000),
			FeeCap: *uint256.NewInt(3000000),
			Gas:    100000,
			Nonce:  2,
		}
		txSlot.IDHash[0] = 2
		txSlots.Append(txSlot, addr[:], true)
		pool.AddRemoteTxs(ctx, txSlots)
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(2), nonce)
	}
	// Remote transactions do not cause pokes
	select {
	case <-ch:
		t.Errorf("remote transactions should not cause re-broadcast")
	default:
	}
}

func TestShanghaiIntrinsicGas(t *testing.T) {
	cases := map[string]struct {
		expected       uint64
		dataLen        uint64
		dataNonZeroLen uint64
		creation       bool
		isShanghai     bool
	}{
		"simple no data": {
			expected:       21000,
			dataLen:        0,
			dataNonZeroLen: 0,
			creation:       false,
			isShanghai:     false,
		},
		"simple with data": {
			expected:       21512,
			dataLen:        32,
			dataNonZeroLen: 32,
			creation:       false,
			isShanghai:     false,
		},
		"creation with data no shanghai": {
			expected:       53512,
			dataLen:        32,
			dataNonZeroLen: 32,
			creation:       true,
			isShanghai:     false,
		},
		"creation with single word and shanghai": {
			expected:       53514, // additional gas for single word
			dataLen:        32,
			dataNonZeroLen: 32,
			creation:       true,
			isShanghai:     true,
		},
		"creation between word 1 and 2 and shanghai": {
			expected:       53532, // additional gas for going into 2nd word although not filling it
			dataLen:        33,
			dataNonZeroLen: 33,
			creation:       true,
			isShanghai:     true,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			gas, reason := CalcIntrinsicGas(c.dataLen, c.dataNonZeroLen, nil, c.creation, true, true, c.isShanghai)
			if reason != Success {
				t.Errorf("expected success but got reason %v", reason)
			}
			if gas != c.expected {
				t.Errorf("expected %v but got %v", c.expected, gas)
			}
		})
	}
}

func TestShanghaiValidateTx(t *testing.T) {
	asrt := assert.New(t)
	tests := map[string]struct {
		expected   DiscardReason
		dataLen    int
		isShanghai bool
	}{
		"no shanghai": {
			expected:   Success,
			dataLen:    32,
			isShanghai: false,
		},
		"shanghai within bounds": {
			expected:   Success,
			dataLen:    32,
			isShanghai: true,
		},
		"shanghai exactly on bound": {
			expected:   Success,
			dataLen:    fixedgas.MaxInitCodeSize,
			isShanghai: true,
		},
		"shanghai one over bound": {
			expected:   InitCodeTooLarge,
			dataLen:    fixedgas.MaxInitCodeSize + 1,
			isShanghai: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ch := make(chan types.Announcements, 100)
			_, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)
			cfg := DefaultConfig

			var shanghaiTime *big.Int
			if test.isShanghai {
				shanghaiTime = big.NewInt(0)
			}

			cache := &kvcache.DummyCache{}
			pool, err := New(ch, coreDB, cfg, cache, *u256.N1, shanghaiTime)
			asrt.NoError(err)
			ctx := context.Background()
			tx, err := coreDB.BeginRw(ctx)
			defer tx.Rollback()
			asrt.NoError(err)

			sndr := sender{nonce: 0, balance: *uint256.NewInt(math.MaxUint64)}
			sndrBytes := make([]byte, types.EncodeSenderLengthForStorage(sndr.nonce, sndr.balance))
			types.EncodeSender(sndr.nonce, sndr.balance, sndrBytes)
			err = tx.Put(kv.PlainState, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sndrBytes)
			asrt.NoError(err)

			txn := &types.TxSlot{
				DataLen:  test.dataLen,
				FeeCap:   *uint256.NewInt(21000),
				Gas:      500000,
				SenderID: 0,
				Creation: true,
			}

			txns := types.TxSlots{
				Txs:     append([]*types.TxSlot{}, txn),
				Senders: types.Addresses{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			}
			err = pool.senders.registerNewSenders(&txns)
			asrt.NoError(err)
			view, err := cache.View(ctx, tx)
			asrt.NoError(err)

			reason := pool.validateTx(txn, false, view)

			if reason != test.expected {
				t.Errorf("expected %v, got %v", test.expected, reason)
			}
		})
	}
}
