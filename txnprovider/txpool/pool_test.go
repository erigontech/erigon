// Copyright 2021 The Erigon Authors
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
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/state"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	accounts3 "github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestNonceFromAddress(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	{
		var txnSlots TxnSlots
		txnSlot1 := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot1.IDHash[0] = 1
		txnSlots.Append(txnSlot1, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}

	{
		txnSlots := TxnSlots{}
		txnSlot2 := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  4,
		}
		txnSlot2.IDHash[0] = 2
		txnSlot3 := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  6,
		}
		txnSlot3.IDHash[0] = 3
		txnSlots.Append(txnSlot2, addr[:], true)
		txnSlots.Append(txnSlot3, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(6), nonce)
	}
	// test too expensive tx
	{
		var txnSlots TxnSlots
		txnSlot1 := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(9 * common.Ether),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot1.IDHash[0] = 4
		txnSlots.Append(txnSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.InsufficientFunds, reason, reason.String())
		}
	}

	// test too low nonce
	{
		var txnSlots TxnSlots
		txnSlot1 := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  1,
		}
		txnSlot1.IDHash[0] = 5
		txnSlots.Append(txnSlot1, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.NonceTooLow, reason, reason.String())
		}
	}
}

func TestMultipleAuthorizations(t *testing.T) {
	addrA := common.HexToAddress("0xa")
	addrB := common.HexToAddress("0xb")
	cases := []struct {
		title          string
		sender         common.Address
		senderNonce    uint64
		authority      *common.Address
		authNonce      uint64
		feecap         uint64
		tipcap         uint64
		expectedReason txpoolcfg.DiscardReason
		replacedAuth   *AuthAndNonce
	}{
		{
			title:          "a setcode txn with sender=A and authority=B",
			sender:         addrA,
			senderNonce:    0,
			authority:      &addrB,
			authNonce:      0,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.Success,
		},
		{
			title:          "A's own authorization with correct sender nonce, and same nonce for authorization",
			sender:         addrA,
			senderNonce:    1,
			authority:      &addrA,
			authNonce:      1,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.NonceTooLow,
		},
		{
			title:          "own authorization with correct sender nonce, and one higher authority nonce",
			sender:         addrA,
			senderNonce:    1,
			authority:      &addrA,
			authNonce:      2,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.Success,
		},
		{
			title:          "A sends senderNonce=(prev auth nonce) wtih existing own authorization of A ",
			sender:         addrA,
			senderNonce:    2,
			authority:      nil,
			authNonce:      0,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.ErrAuthorityReserved,
		},

		{
			title:          "B sends with nonce as B's auth nonce of existing authorization sent by A",
			sender:         addrB,
			senderNonce:    0,
			authority:      nil,
			authNonce:      0,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.ErrAuthorityReserved,
		},
		{
			title:          "B sends non-setcode txn senderNonce=(auth nonce + 1) with existing own authorization sent by A",
			sender:         addrB,
			senderNonce:    1,
			authority:      nil,
			authNonce:      0,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.Success,
		},
		{
			title:          "B's existing authorization in pool, B sends new setcode txn with higher nonce and auth nonce",
			sender:         addrB,
			senderNonce:    2,
			authority:      &addrB,
			authNonce:      3,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.Success,
		},
		{
			title:          "replace setcode txn sent By B with B's authorization, with higher tipcap and A's authorization",
			sender:         addrB,
			senderNonce:    2,
			authority:      &addrA,
			authNonce:      3,
			feecap:         200_000,
			tipcap:         200_000,
			expectedReason: txpoolcfg.Success,
			replacedAuth:   &AuthAndNonce{addrB.String(), 3},
		},
		{
			title:          "B sends to replace own setcode txn with non setcode txn, with higher tipcap",
			sender:         addrB,
			senderNonce:    2,
			authority:      nil,
			authNonce:      0,
			feecap:         300_000,
			tipcap:         300_000,
			expectedReason: txpoolcfg.Success,
			replacedAuth:   &AuthAndNonce{addrA.String(), 3},
		},
		{
			title:          "B sends to replace non setcode txn, with setcode txn (A's auth) with higher tipcap",
			sender:         addrB,
			senderNonce:    2,
			authority:      &addrA,
			authNonce:      3,
			feecap:         400_000,
			tipcap:         400_000,
			expectedReason: txpoolcfg.Success,
		},
		{
			title:          "B sends another setcode txn with B's authorization after setcode txn for A's authorization",
			sender:         addrB,
			senderNonce:    3,
			authority:      &addrB,
			authNonce:      4,
			feecap:         100_000,
			tipcap:         100_000,
			expectedReason: txpoolcfg.Success,
		},
	}

	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, common.Big0 /* shanghaiTime */, nil /* agraBlock */, common.Big0 /* cancunTime */, common.Big0 /* pragueTime */, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)
	require.NotEqual(t, pool, nil)

	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(50_000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       36_000_000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	require.NoError(t, err)

	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(10 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addrA),
		Data:    v,
	})
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addrB),
		Data:    v,
	})

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(t, err)

	idHash := 0
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			t.Log("\n--- Testing " + c.title)
			var txnSlots TxnSlots
			txnSlot1 := &TxnSlot{
				Tip:    *uint256.NewInt(c.tipcap),
				FeeCap: *uint256.NewInt(c.feecap),
				Gas:    100000,
				Nonce:  c.senderNonce,
			}
			if c.authority != nil {
				txnSlot1.AuthAndNonces = []AuthAndNonce{{c.authority.String(), c.authNonce}}
				txnSlot1.Type = SetCodeTxnType
			}
			txnSlot1.IDHash[0] = uint8(idHash)
			idHash++
			txnSlots.Append(txnSlot1, c.sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txnSlots)
			require.NoError(t, err)
			assert.Equal(t, []txpoolcfg.DiscardReason{c.expectedReason}, reasons)
			if c.authority != nil && c.expectedReason == txpoolcfg.Success {
				_, ok := pool.auths[AuthAndNonce{c.authority.String(), c.authNonce}]
				assert.True(t, ok)
			}
			if c.replacedAuth != nil {
				_, ok := pool.auths[*c.replacedAuth]
				assert.False(t, ok)
			}
		})
	}
}

func TestRecoverSignerFromRLP_ValidData(t *testing.T) {
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubKey := crypto.PubkeyToAddress(privateKey.PublicKey)
	chainID := uint64(7078815900)

	var b [33]byte
	data := bytes.NewBuffer(b[:])
	data.Reset()

	// Encode RLP data exactly as in the previous implementation
	authLen := rlp.U64Len(chainID)
	authLen += 1 + length.Addr
	authLen += rlp.U64Len(0) // nonce
	require.NoError(t, rlp.EncodeStructSizePrefix(authLen, data, b[:]))
	require.NoError(t, rlp.EncodeInt(chainID, data, b[:]))
	require.NoError(t, rlp.EncodeOptionalAddress(&pubKey, data, b[:]))
	require.NoError(t, rlp.EncodeInt(0, data, b[:]))

	// Prepare hash data exactly as before
	hashData := []byte{params.SetCodeMagicPrefix}
	hashData = append(hashData, data.Bytes()...)
	hash := crypto.Keccak256Hash(hashData)

	// Sign the hash
	sig, err := crypto.Sign(hash.Bytes(), privateKey)
	require.NoError(t, err)

	// Separate signature components
	r := uint256.NewInt(0).SetBytes(sig[:32])
	s := uint256.NewInt(0).SetBytes(sig[32:64])
	yParity := sig[64]

	// Recover signer using the explicit RecoverSignerFromRLP function
	recoveredAddress, err := types.RecoverSignerFromRLP(data.Bytes(), yParity, *r, *s)
	require.NoError(t, err)
	assert.NotNil(t, recoveredAddress)

	// Verify the recovered address matches the original public key address
	assert.Equal(t, pubKey, *recoveredAddress)
}

func TestReplaceWithHigherFee(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotNil(pool)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}
	// Bumped only feeCap, transaction not accepted
	{
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(3000000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 2
		txnSlots.Append(txnSlot, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.NotReplaced, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(3), nonce)
	}
	// Bumped only tip, transaction not accepted
	{
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(3000000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 3
		txnSlots.Append(txnSlot, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.NotReplaced, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(3), nonce)
	}
	// Bumped both tip and feeCap by 10%, txn accepted
	{
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(330000),
			FeeCap: *uint256.NewInt(330000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 4
		txnSlots.Append(txnSlot, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
		nonce, ok := pool.NonceFromAddress(addr)
		assert.True(ok)
		assert.Equal(uint64(3), nonce)
	}
}

func TestReverseNonces(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(1_000_000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)
	// 1. Send high fee transaction with nonce gap
	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(500_000),
			FeeCap: *uint256.NewInt(3_000_000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}
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
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(500_000),
			FeeCap: *uint256.NewInt(500_000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 2
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}
	select {
	case annoucements := <-ch:
		for i := 0; i < annoucements.Len(); i++ {
			_, _, hash := annoucements.At(i)
			fmt.Printf("propagated hash %x\n", hash)
		}
	default:

	}

	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(600_000),
			FeeCap: *uint256.NewInt(3_000_000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 3
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}
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
// this is a workaround for cases when transactions are getting stuck for strange reasons
// even though logs show they are broadcast
func TestTxnPoke(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	var idHash Hashes
	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 1
		idHash = append(idHash, txnSlot.IDHash[:]...)
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}
	var promoted Announcements
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
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.DuplicateHash, reason, reason.String())
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
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(3000000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 2
		txnSlots.Append(txnSlot, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.NotReplaced, reason, reason.String())
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
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)
		pool.AddRemoteTxns(ctx, txnSlots)
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
		txnSlots := TxnSlots{}
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(3000000),
			FeeCap: *uint256.NewInt(3000000),
			Gas:    100000,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 2
		txnSlots.Append(txnSlot, addr[:], true)
		pool.AddRemoteTxns(ctx, txnSlots)
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

func TestShanghaiValidateTxn(t *testing.T) {
	asrt := assert.New(t)
	tests := map[string]struct {
		expected   txpoolcfg.DiscardReason
		dataLen    int
		isShanghai bool
		creation   bool
	}{
		"no shanghai": {
			expected:   txpoolcfg.Success,
			dataLen:    32,
			isShanghai: false,
			creation:   true,
		},
		"shanghai within bounds": {
			expected:   txpoolcfg.Success,
			dataLen:    32,
			isShanghai: true,
			creation:   true,
		},
		"shanghai exactly on bound - create tx": {
			expected:   txpoolcfg.Success,
			dataLen:    params.MaxInitCodeSize,
			isShanghai: true,
			creation:   true,
		},
		"shanghai one over bound - create tx": {
			expected:   txpoolcfg.InitCodeTooLarge,
			dataLen:    params.MaxInitCodeSize + 1,
			isShanghai: true,
			creation:   true,
		},
		"shanghai exactly on bound - calldata tx": {
			expected:   txpoolcfg.Success,
			dataLen:    params.MaxInitCodeSize,
			isShanghai: true,
			creation:   false,
		},
		"shanghai one over bound - calldata tx": {
			expected:   txpoolcfg.Success,
			dataLen:    params.MaxInitCodeSize + 1,
			isShanghai: true,
			creation:   false,
		},
	}

	logger := log.New()

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ch := make(chan Announcements, 100)
			coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

			cfg := txpoolcfg.DefaultConfig

			var shanghaiTime *big.Int
			if test.isShanghai {
				shanghaiTime = big.NewInt(0)
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tx, err := coreDB.BeginTemporalRw(ctx)
			defer tx.Rollback()
			asrt.NoError(err)
			sd, err := state.NewSharedDomains(tx, logger)
			asrt.NoError(err)
			defer sd.Close()
			cache := kvcache.NewDummy()
			pool, err := New(ctx, ch, nil, coreDB, cfg, cache, *u256.N1, shanghaiTime, nil /* agraBlock */, nil /* cancunTime */, nil, nil, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
			asrt.NoError(err)

			sndr := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(math.MaxUint64)}
			sndrBytes := accounts3.SerialiseV3(&sndr)
			err = sd.DomainPut(kv.AccountsDomain, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil, sndrBytes, nil, 0)
			asrt.NoError(err)

			err = sd.Flush(ctx, tx)
			asrt.NoError(err)

			txn := &TxnSlot{
				DataLen:  test.dataLen,
				FeeCap:   *uint256.NewInt(21000),
				Gas:      500000,
				SenderID: 0,
				Creation: test.creation,
			}

			txns := TxnSlots{
				Txns:    append([]*TxnSlot{}, txn),
				Senders: Addresses{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			}
			err = pool.senders.registerNewSenders(&txns, logger)
			asrt.NoError(err)
			view, err := cache.View(ctx, tx)
			asrt.NoError(err)
			pool.blockGasLimit.Store(30_000_000)
			reason := pool.validateTx(txn, false, view)

			if reason != test.expected {
				t.Errorf("expected %v, got %v", test.expected, reason)
			}
		})
	}
}

func TestTooHighGasLimitTxnValidation(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    1000001,
			Nonce:  2,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Len(reasons, 1)
		assert.Equal(txpoolcfg.GasLimitTooHigh, reasons[0])
	}
}

func TestSetCodeTxnValidationWithLargeAuthorizationValues(t *testing.T) {
	maxUint256 := new(uint256.Int).SetAllOne()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan Announcements, 1)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	cfg := txpoolcfg.DefaultConfig
	chainID := *maxUint256
	cache := kvcache.NewDummy()
	logger := log.New()
	pool, err := New(ctx, ch, nil, coreDB, cfg, cache, chainID, common.Big0 /* shanghaiTime */, nil, /* agraBlock */
		common.Big0 /* cancunTime */, common.Big0 /* pragueTime */, nil, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
	require.NoError(t, err)
	pool.blockGasLimit.Store(30_000_000)
	tx, err := coreDB.BeginTemporalRw(ctx)
	defer tx.Rollback()
	require.NoError(t, err)
	sd, err := state.NewSharedDomains(tx.(kv.TemporalTx), logger)
	require.NoError(t, err)
	defer sd.Close()

	sndr := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(math.MaxUint64)}
	sndrBytes := accounts3.SerialiseV3(&sndr)
	err = sd.DomainPut(kv.AccountsDomain, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil, sndrBytes, nil, 0)
	require.NoError(t, err)

	err = sd.Flush(ctx, tx)
	require.NoError(t, err)

	txn := &TxnSlot{
		FeeCap:        *uint256.NewInt(21000),
		Gas:           500000,
		SenderID:      0,
		Type:          SetCodeTxnType,
		AuthAndNonces: []AuthAndNonce{{nonce: 0, authority: common.Address{}.String()}},
	}

	txns := TxnSlots{
		Txns:    append([]*TxnSlot{}, txn),
		Senders: Addresses{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	err = pool.senders.registerNewSenders(&txns, logger)
	require.NoError(t, err)
	view, err := cache.View(ctx, tx)
	require.NoError(t, err)

	result := pool.validateTx(txn, false /* isLocal */, view)
	assert.Equal(t, txpoolcfg.Success, result)
}

// Blob gas price bump + other requirements to replace existing txns in the pool
func TestBlobTxnReplacement(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, common.Big0, nil, common.Big0, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)

	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:       stateVersionID,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1

	// Add 1 eth to the user account, as a part of change
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)

	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	tip, feeCap, blobFeeCap := uint256.NewInt(100_000), uint256.NewInt(200_000), uint256.NewInt(200_000)

	//add a blob txn to the pool
	{
		txnSlots := TxnSlots{}
		blobTxn := makeBlobTxn()

		blobTxn.IDHash[0] = 0x00
		blobTxn.Nonce = 0x2
		txnSlots.Append(&blobTxn, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		t.Logf("Reasons %v", reasons)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}

	{
		// try to replace it with 5% extra blob gas, 2x higher txn fee - should fail
		txnSlots := TxnSlots{}
		blobTxn := makeBlobTxn()
		blobTxn.Nonce = 0x2
		blobTxn.FeeCap.Mul(uint256.NewInt(2), feeCap)
		blobTxn.Tip.Mul(uint256.NewInt(2), tip)
		//increase blobFeeCap by 10% - no good
		blobTxn.BlobFeeCap.Add(blobFeeCap, uint256.NewInt(1).Div(blobFeeCap, uint256.NewInt(10)))
		blobTxn.IDHash[0] = 0x01
		txnSlots.Append(&blobTxn, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		t.Logf("Reasons %v", reasons)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.ReplaceUnderpriced, reason, reason.String())
		}
	}

	{
		txnSlots := TxnSlots{}
		//try to replace it with a regular txn - should fail
		regularTxn := TxnSlot{
			DataLen:    32,
			FeeCap:     *uint256.NewInt(1).Mul(uint256.NewInt(10), feeCap), //10x the previous
			Tip:        *uint256.NewInt(1).Mul(uint256.NewInt(10), tip),
			BlobFeeCap: *uint256.NewInt(1).Mul(uint256.NewInt(10), blobFeeCap),
			Gas:        500000,
			SenderID:   0,
			Creation:   true,
			Nonce:      0x2,
		}
		regularTxn.IDHash[0] = 0x02
		txnSlots.Append(&regularTxn, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		t.Logf("Reasons %v", reasons)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.BlobTxReplace, reason, reason.String())
		}
	}

	// Try to replace it with required price bump (configured in pool.cfg.BlobPriceBump for blob txns) to all transaction fields - should be successful only if all are bumped
	{
		blobTxn := makeBlobTxn()
		origTip := blobTxn.Tip
		origFee := blobTxn.FeeCap
		blobTxn.Nonce = 0x2
		blobTxn.IDHash[0] = 0x03
		txnSlots := TxnSlots{}
		txnSlots.Append(&blobTxn, addr[:], true)

		// Get the config of the pool for BlobPriceBump and bump prices
		requiredPriceBump := pool.cfg.BlobPriceBump

		// Bump the tip only
		blobTxn.Tip.MulDivOverflow(tip, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.ReplaceUnderpriced, reasons[0], reasons[0].String())

		// Bump the fee + tip
		blobTxn.FeeCap.MulDivOverflow(feeCap, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.ReplaceUnderpriced, reasons[0], reasons[0].String())

		// Bump only Feecap
		blobTxn.Tip = origTip
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.ReplaceUnderpriced, reasons[0], reasons[0].String())

		// Bump fee cap + blobFee cap
		blobTxn.BlobFeeCap.MulDivOverflow(blobFeeCap, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.NotReplaced, reasons[0], reasons[0].String())

		// Bump only blobFee cap
		blobTxn.FeeCap = origFee
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.NotReplaced, reasons[0], reasons[0].String())

		// Bump all prices
		blobTxn.Tip.MulDivOverflow(tip, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		blobTxn.FeeCap.MulDivOverflow(feeCap, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.Success, reasons[0], reasons[0].String())
	}
}

// Todo, make the txn more realistic with good values
func makeBlobTxn() TxnSlot {
	wrapperRlp, commitments := types.MakeBlobTxnRlp()
	commitment0 := commitments[0]
	commitment1 := commitments[1]
	tip, feeCap, blobFeeCap := uint256.NewInt(100_000), uint256.NewInt(200_000), uint256.NewInt(200_000)
	blobTxn := TxnSlot{}
	tctx := NewTxnParseContext(*uint256.NewInt(5))
	tctx.WithSender(false)
	tctx.ParseTransaction(wrapperRlp, 0, &blobTxn, nil, false, true, nil)
	blobTxn.BlobHashes = make([]common.Hash, 2)
	blobTxn.BlobHashes[0] = common.Hash(kzg.KZGToVersionedHash(commitment0))
	blobTxn.BlobHashes[1] = common.Hash(kzg.KZGToVersionedHash(commitment1))
	blobTxn.Tip = *tip
	blobTxn.FeeCap = *feeCap
	blobTxn.BlobFeeCap = *blobFeeCap
	return blobTxn
}

func makeblobTxnWithCellProofs() TxnSlot {
	return TxnSlot{}
}

func TestBlobValidation(t *testing.T) {

}

func TestDropRemoteAtNoGossip(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)

	cfg := txpoolcfg.DefaultConfig
	cfg.NoGossip = true

	logger := log.New()
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	txnPool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, big.NewInt(0), big.NewInt(0), nil, nil, nil, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(txnPool, nil)

	err = txnPool.start(ctx)
	require.NoError(err)

	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(1_000_000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = txnPool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)
	// 1. Try Local TxnSlot
	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(500_000),
			FeeCap: *uint256.NewInt(3_000_000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)

		reasons, err := txnPool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}
	select {
	case annoucements := <-ch:
		for i := 0; i < annoucements.Len(); i++ {
			_, _, hash := annoucements.At(i)
			fmt.Printf("propagated hash %x\n", hash)
		}
	default:

	}

	// 2. Try same TxnSlot, but as remote; txn must be dropped
	{
		var txnSlots TxnSlots
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(500_000),
			FeeCap: *uint256.NewInt(3_000_000),
			Gas:    100000,
			Nonce:  3,
		}
		txnSlot.IDHash[0] = 1
		txnSlots.Append(txnSlot, addr[:], true)

		txnPool.AddRemoteTxns(ctx, txnSlots)
	}

	// empty because AddRemoteTxns logic is intentionally empty
	assert.Empty(txnPool.unprocessedRemoteByHash)
	assert.Empty(txnPool.unprocessedRemoteTxns.Txns)

	require.NoError(txnPool.processRemoteTxns(ctx))

	checkAnnouncementEmpty := func() bool {
		select {
		case <-ch:
			return false
		default:
			return true
		}
	}
	// no announcement because unprocessedRemoteTxns is already empty
	assert.True(checkAnnouncementEmpty())
}

func TestBlobSlots(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	//Setting limits for blobs in the pool
	cfg.TotalBlobPoolLimit = 20

	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, common.Big0, nil, common.Big0, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:       stateVersionID,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte

	// Add 1 eth to the user account, as a part of change
	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)

	for i := 0; i < 11; i++ {
		addr[0] = uint8(i + 1)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	//Adding 20 blobs from 10 different accounts
	for i := 0; i < int(cfg.TotalBlobPoolLimit/2); i++ {
		txnSlots := TxnSlots{}
		addr[0] = uint8(i + 1)
		blobTxn := makeBlobTxn() // makes a txn with 2 blobs
		blobTxn.IDHash[0] = uint8(2*i + 1)
		blobTxn.Nonce = 0
		txnSlots.Append(&blobTxn, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		for _, reason := range reasons {
			assert.Equal(txpoolcfg.Success, reason, reason.String())
		}
	}

	// Adding another blob txn should reject
	txnSlots := TxnSlots{}
	addr[0] = 11
	blobTxn := makeBlobTxn()
	blobTxn.IDHash[0] = uint8(21)
	blobTxn.Nonce = 0

	txnSlots.Append(&blobTxn, addr[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txnSlots)
	require.NoError(err)
	for _, reason := range reasons {
		assert.Equal(txpoolcfg.BlobPoolOverflow, reason, reason.String())
	}
}

func TestGetBlobsV1(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	//Setting limits for blobs in the pool
	cfg.TotalBlobPoolLimit = 20

	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, common.Big0, nil, common.Big0, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	pool.blockGasLimit.Store(30000000)
	var stateVersionID uint64 = 0

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:       stateVersionID,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte

	// Add 1 eth to the user account, as a part of change
	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)

	for i := 0; i < 11; i++ {
		addr[0] = uint8(i + 1)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)
	blobHashes := make([]common.Hash, 0, 20)

	//Adding 2 blobs with 1 txn
	txnSlots := TxnSlots{}
	addr[0] = uint8(1)
	blobTxn := makeBlobTxn() // makes a txn with 2 blobs
	blobTxn.IDHash[0] = uint8(3)
	blobTxn.Nonce = 0
	blobTxn.Gas = 50000
	txnSlots.Append(&blobTxn, addr[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txnSlots)
	require.NoError(err)
	for _, reason := range reasons {
		assert.Equal(txpoolcfg.Success, reason, reason.String())
	}
	blobHashes = append(blobHashes, blobTxn.BlobHashes...)

	blobBundles := pool.GetBlobs(blobHashes)
	require.Equal(len(blobBundles), len(blobHashes))
	blobs := make([][]byte, len(blobBundles))
	proofs := make([][]byte, len(blobBundles))
	for _, bb := range blobBundles {
		blobs = append(blobs, bb.Blob)
		for _, p := range bb.Proofs{
			proofs = append(proofs, p[:])
		}
		
	}
	require.Equal(len(proofs), len(blobHashes))
	assert.Equal(blobTxn, blobs)
	assert.Equal(blobTxn.BlobBundles[0].Proofs[0], proofs[0])
	assert.Equal(blobTxn.BlobBundles[1].Proofs[0], proofs[1])
	

}

func TextGetBlobsV2(t *testing.T) {

}

func TestGasLimitChanged(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       50_000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
		Action:  remote.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	var txnSlots TxnSlots
	txnSlot1 := &TxnSlot{
		Tip:    *uint256.NewInt(300000),
		FeeCap: *uint256.NewInt(300000),
		Gas:    100_000,
		Nonce:  3,
	}
	txnSlot1.IDHash[0] = 1
	txnSlots.Append(txnSlot1, addr[:], true)

	reasons, err := pool.AddLocalTxns(ctx, txnSlots)
	require.NoError(err)
	for _, reason := range reasons {
		assert.Equal(txpoolcfg.GasLimitTooHigh, reason)
	}

	change.ChangeBatch[0].Changes = nil
	change.BlockGasLimit = 150_000
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	reasons, err = pool.AddLocalTxns(ctx, txnSlots)
	require.NoError(err)

	for _, reason := range reasons {
		assert.Equal(txpoolcfg.Success, reason, reason.String())
	}
}

// sender - immutable structure which stores only nonce and balance of account
type sender struct {
	balance uint256.Int
	nonce   uint64
}

func newSender(nonce uint64, balance uint256.Int) *sender {
	return &sender{nonce: nonce, balance: balance}
}

func BenchmarkProcessRemoteTxns(b *testing.B) {
	require := require.New(b)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	db := memdb.NewTestPoolDB(b)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, nil, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)

	// Start the transaction pool
	err = pool.start(ctx)
	require.NoError(err)

	// Set up initial blockchain state
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remote.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remote.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}

	// Create 100 test accounts with 1 ETH balance each
	for i := 0; i < 100; i++ {
		var addr [20]byte
		addr[0] = uint8(i + 1)
		acc := accounts3.Account{
			Nonce:       0,
			Balance:     *uint256.NewInt(1 * common.Ether),
			CodeHash:    common.Hash{},
			Incarnation: 1,
		}
		v := accounts3.SerialiseV3(&acc)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}

	// Apply the initial state to the pool
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	// Create test transactions for benchmarking
	var testTxns TxnSlots
	for i := 0; i < b.N; i++ {
		var addr [20]byte
		addr[0] = uint8(i%100 + 1) // Use one of our test accounts
		txnSlot := &TxnSlot{
			Tip:    *uint256.NewInt(300000),
			FeeCap: *uint256.NewInt(300000),
			Gas:    100000,
			Nonce:  uint64(i / 100), // Different nonce for each account
		}
		txnSlot.IDHash[0] = uint8(i + 1)
		testTxns.Append(txnSlot, addr[:], true)
	}

	b.ResetTimer()

	// Run the benchmark: process transactions one by one
	// This measures the performance of adding and processing remote transactions
	for i := 0; i < b.N; i++ {
		pool.AddRemoteTxns(ctx, TxnSlots{testTxns.Txns[i : i+1], testTxns.Senders[i : i+1], testTxns.IsLocal[i : i+1]})
		err := pool.processRemoteTxns(ctx)
		require.NoError(err)
	}

	b.StopTimer()

	// Log final pool statistics after processing all transactions
	pending, baseFee, queued := pool.CountContent()
	b.Logf("Final pool stats - pending: %d, baseFee: %d, queued: %d", pending, baseFee, queued)
}
