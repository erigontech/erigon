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
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func newTestTxnSlot(nonce uint64, senderID uint64, tip, feeCap uint64, gas uint64) *TxnSlot {
	to := common.Address{1} // non-nil To means not a contract creation
	txn := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    nonce,
			GasLimit: gas,
			To:       &to,
		},
		TipCap: *uint256.NewInt(tip),
		FeeCap: *uint256.NewInt(feeCap),
	}
	return &TxnSlot{
		Txn:      txn,
		Nonce:    nonce,
		SenderID: senderID,
	}
}

func newTestSetCodeTxnSlot(nonce uint64, senderID uint64, tip, feeCap uint64, gas uint64) *TxnSlot {
	to := common.Address{1} // non-nil To means not a contract creation
	txn := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    nonce,
				GasLimit: gas,
				To:       &to,
			},
			TipCap: *uint256.NewInt(tip),
			FeeCap: *uint256.NewInt(feeCap),
		},
	}
	return &TxnSlot{
		Txn:      txn,
		Nonce:    nonce,
		SenderID: senderID,
	}
}

func TestNonceFromAddress(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		txnSlot1 := newTestTxnSlot(3, 0, 300000, 300000, 100000)
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
		txnSlot2 := newTestTxnSlot(4, 0, 300000, 300000, 100000)
		txnSlot2.IDHash[0] = 2
		txnSlot3 := newTestTxnSlot(6, 0, 300000, 300000, 100000)
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
		txnSlot1 := newTestTxnSlot(3, 0, 300000, 9*common.Ether, 100000)
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
		txnSlot1 := newTestTxnSlot(1, 0, 300000, 300000, 100000)
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
			replacedAuth:   &AuthAndNonce{addrB, 3},
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
			replacedAuth:   &AuthAndNonce{addrA, 3},
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Prague"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)
	require.NotEqual(t, pool, nil)

	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(50_000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       36_000_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	require.NoError(t, err)

	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(10 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addrA),
		Data:    v,
	}, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
			var txnSlot1 *TxnSlot
			if c.authority != nil {
				txnSlot1 = newTestSetCodeTxnSlot(c.senderNonce, 0, c.tipcap, c.feecap, 100000)
				txnSlot1.AuthAndNonces = []AuthAndNonce{{*c.authority, c.authNonce}}
			} else {
				txnSlot1 = newTestTxnSlot(c.senderNonce, 0, c.tipcap, c.feecap, 100000)
			}
			txnSlot1.IDHash[0] = uint8(idHash)
			idHash++
			txnSlots.Append(txnSlot1, c.sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, txnSlots)
			require.NoError(t, err)
			assert.Equal(t, []txpoolcfg.DiscardReason{c.expectedReason}, reasons)
			if c.authority != nil && c.expectedReason == txpoolcfg.Success {
				_, ok := pool.auths[AuthAndNonce{*c.authority, c.authNonce}]
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
	require.NoError(t, rlp.EncodeListPrefix(authLen, data, b[:]))
	require.NoError(t, rlp.EncodeU64(chainID, data, b[:]))
	require.NoError(t, types.EncodeOptionalAddress(&pubKey, data, b[:]))
	require.NoError(t, rlp.EncodeU64(0, data, b[:]))

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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotNil(pool)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		txnSlot := newTestTxnSlot(3, 0, 300000, 300000, 100000)
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
		txnSlot := newTestTxnSlot(3, 0, 300000, 3000000, 100000)
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
		txnSlot := newTestTxnSlot(3, 0, 3000000, 300000, 100000)
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
		txnSlot := newTestTxnSlot(3, 0, 330000, 330000, 100000)
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(1_000_000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		txnSlot := newTestTxnSlot(3, 0, 500_000, 3_000_000, 100000)
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
		txnSlot := newTestTxnSlot(2, 0, 500_000, 500_000, 100000)
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
		txnSlot := newTestTxnSlot(2, 0, 600_000, 3_000_000, 100000)
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		txnSlot := newTestTxnSlot(2, 0, 300000, 300000, 100000)
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
		txnSlot := newTestTxnSlot(2, 0, 300000, 300000, 100000)
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
		txnSlot := newTestTxnSlot(2, 0, 3000000, 300000, 100000)
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
		txnSlot := newTestTxnSlot(2, 0, 300000, 300000, 100000)
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
		txnSlot := newTestTxnSlot(2, 0, 3000000, 3000000, 100000)
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
	if testing.Short() {
		t.Skip("slow test")
	}
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

			chainConfig := testforks.Forks["Paris"]
			if test.isShanghai {
				chainConfig = testforks.Forks["Shanghai"]
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tx, err := coreDB.BeginTemporalRw(ctx)
			asrt.NoError(err)
			defer tx.Rollback()
			sd, err := execctx.NewSharedDomains(ctx, tx, logger)
			asrt.NoError(err)
			defer sd.Close()
			cache := kvcache.NewDummy()
			pool, err := New(ctx, ch, nil, coreDB, cfg, cache, chainConfig, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
			asrt.NoError(err)

			sndr := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(math.MaxUint64)}
			sndrBytes := accounts3.SerialiseV3(&sndr)
			txNum := uint64(0)
			err = sd.DomainPut(kv.AccountsDomain, tx, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sndrBytes, txNum, nil)
			asrt.NoError(err)

			err = sd.Flush(ctx, tx)
			asrt.NoError(err)

			var to *common.Address
			if !test.creation {
				to = &common.Address{}
			}
			txn := &TxnSlot{
				Txn: &types.DynamicFeeTransaction{
					CommonTx: types.CommonTx{
						GasLimit: 500000,
						Data:     make([]byte, test.dataLen),
						To:       to,
					},
					FeeCap: *uint256.NewInt(21000),
				},
				SenderID: 0,
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		txnSlot := newTestTxnSlot(2, 0, 300000, 300000, 1000001)
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
	var chainConfig chain.Config
	copier.Copy(&chainConfig, testforks.Forks["Prague"])
	chainConfig.ChainID = maxUint256.ToBig()
	cache := kvcache.NewDummy()
	logger := log.New()
	pool, err := New(ctx, ch, nil, coreDB, cfg, cache, &chainConfig, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
	require.NoError(t, err)
	pool.blockGasLimit.Store(30_000_000)
	tx, err := coreDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, tx.(kv.TemporalTx), logger)
	require.NoError(t, err)
	defer sd.Close()

	sndr := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(math.MaxUint64)}
	sndrBytes := accounts3.SerialiseV3(&sndr)
	txNum := uint64(0)
	err = sd.DomainPut(kv.AccountsDomain, tx, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, sndrBytes, txNum, nil)
	require.NoError(t, err)

	err = sd.Flush(ctx, tx)
	require.NoError(t, err)

	txn := newTestSetCodeTxnSlot(0, 0, 0, 21000, 500000)
	txn.AuthAndNonces = []AuthAndNonce{{nonce: 0, authority: common.Address{}}}

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
	t.Parallel()
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)

	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:       stateVersionID,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1

	// Add 1 eth to the user account, as a part of change
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)

	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		w := blobTxn.Txn.(*types.BlobTx)
		w.FeeCap.Mul(uint256.NewInt(2), feeCap)
		w.TipCap.Mul(uint256.NewInt(2), tip)
		//increase blobFeeCap by 10% - no good
		w.MaxFeePerBlobGas.Add(blobFeeCap, uint256.NewInt(1).Div(blobFeeCap, uint256.NewInt(10)))
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
		regularTxn := &TxnSlot{
			Txn: &types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    0x2,
					GasLimit: 500000,
					Data:     make([]byte, 32),
				},
				FeeCap: *uint256.NewInt(1).Mul(uint256.NewInt(10), feeCap),
				TipCap: *uint256.NewInt(1).Mul(uint256.NewInt(10), tip),
			},
			Nonce:    0x2,
			SenderID: 0,
		}
		regularTxn.IDHash[0] = 0x02
		txnSlots.Append(regularTxn, addr[:], true)
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
		w := blobTxn.Txn.(*types.BlobTx)
		origTip := w.TipCap
		origFee := w.FeeCap
		blobTxn.Nonce = 0x2
		blobTxn.IDHash[0] = 0x03
		txnSlots := TxnSlots{}
		txnSlots.Append(&blobTxn, addr[:], true)

		// Get the config of the pool for BlobPriceBump and bump prices
		requiredPriceBump := pool.cfg.BlobPriceBump

		// Bump the tip only
		w.TipCap.MulDivOverflow(tip, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.ReplaceUnderpriced, reasons[0], reasons[0].String())

		// Bump the fee + tip
		w.FeeCap.MulDivOverflow(feeCap, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.ReplaceUnderpriced, reasons[0], reasons[0].String())

		// Bump only Feecap
		w.TipCap = origTip
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.ReplaceUnderpriced, reasons[0], reasons[0].String())

		// Bump fee cap + blobFee cap
		w.MaxFeePerBlobGas.MulDivOverflow(blobFeeCap, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.NotReplaced, reasons[0], reasons[0].String())

		// Bump only blobFee cap
		w.FeeCap = origFee
		reasons, err = pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		assert.Equal(txpoolcfg.NotReplaced, reasons[0], reasons[0].String())

		// Bump all prices
		w.TipCap.MulDivOverflow(tip, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
		w.FeeCap.MulDivOverflow(feeCap, uint256.NewInt(requiredPriceBump+100), uint256.NewInt(100))
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
	// Set blob hashes and fee fields on the underlying transaction
	bt := blobTxn.Txn.(*types.BlobTx)
	bt.BlobVersionedHashes = make([]common.Hash, 2)
	bt.BlobVersionedHashes[0] = common.Hash(kzg.KZGToVersionedHash(commitment0))
	bt.BlobVersionedHashes[1] = common.Hash(kzg.KZGToVersionedHash(commitment1))
	bt.TipCap = *tip
	bt.FeeCap = *feeCap
	bt.MaxFeePerBlobGas = *blobFeeCap
	return blobTxn
}

func makeWrappedBlobTxnRlpWithCellProofs(t *testing.T, chainID *uint256.Int, blobCount int) []byte {
	t.Helper()

	require := require.New(t)
	tipCap := uint256.NewInt(2 * common.GWei)
	feeCap := uint256.NewInt(30 * common.GWei)
	maxFeePerBlobGas := uint256.NewInt(500_000)

	wrapper := &types.BlobTxWrapper{
		Tx: types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    0,
					To:       &common.Address{1},
					GasLimit: 1_000_000,
					Value:    *uint256.NewInt(0),
					Data:     []byte{0x01},
				},
				ChainID: *chainID,
				TipCap:  *tipCap,
				FeeCap:  *feeCap,
			},
			MaxFeePerBlobGas: *maxFeePerBlobGas,
		},
		WrapperVersion: 1,
		Commitments:    make(types.BlobKzgs, blobCount),
		Blobs:          make(types.Blobs, blobCount),
		Proofs:         make(types.KZGProofs, 0, blobCount*int(params.CellsPerExtBlob)),
	}

	kzgCtx := kzg.Ctx()
	for i := 0; i < blobCount; i++ {
		for j := range wrapper.Blobs[i] {
			wrapper.Blobs[i][j] = byte(i + 1)
		}
		commitment, err := kzgCtx.BlobToKZGCommitment((*goethkzg.Blob)(&wrapper.Blobs[i]), 0)
		require.NoError(err)
		_, cellProofs, err := kzgCtx.ComputeCellsAndKZGProofs((*goethkzg.Blob)(&wrapper.Blobs[i]), 4)
		require.NoError(err)

		copy(wrapper.Commitments[i][:], commitment[:])
		for _, proof := range &cellProofs {
			var proofBytes types.KZGProof
			copy(proofBytes[:], proof[:])
			wrapper.Proofs = append(wrapper.Proofs, proofBytes)
		}

		wrapper.Tx.BlobVersionedHashes = append(wrapper.Tx.BlobVersionedHashes, common.Hash(kzg.KZGToVersionedHash(commitment)))
	}

	key, err := crypto.GenerateKey()
	require.NoError(err)
	signedTx, err := types.SignTx(wrapper, *types.LatestSignerForChainID(chainID.ToBig()), key)
	require.NoError(err)
	dt := &wrapper.Tx.DynamicFeeTransaction
	v, r, s := signedTx.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	buf := bytes.NewBuffer(nil)
	require.NoError(wrapper.MarshalBinaryWrapped(buf))

	return buf.Bytes()
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
	txnPool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Shanghai"], nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(txnPool, nil)

	err = txnPool.start(ctx)
	require.NoError(err)

	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(1_000_000)
	// start blocks from 0, set empty hash - then kvcache will also work on this
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{
		Nonce:       2,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
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
		txnSlot := newTestTxnSlot(3, 0, 500_000, 3_000_000, 100000)
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
		txnSlot := newTestTxnSlot(3, 0, 500_000, 3_000_000, 100000)
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
	if testing.Short() {
		t.Skip("slow test")
	}
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	var stateVersionID uint64 = 0

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:       stateVersionID,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte

	// Add 1 eth to the user account, as a part of change
	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)

	for i := 0; i < 11; i++ {
		addr[0] = uint8(i + 1)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
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

// TestOsakaProofShapeMismatchDiscardsCompletely verifies that when Osaka activates,
// pre-Osaka-shape blob transactions are fully discarded by best() — not just removed
// from the Pending sub-pool. This ensures totalBlobsInPool and byHash are cleaned up,
// so new Osaka-shaped blob transactions can be admitted.
func TestOsakaProofShapeMismatchDiscardsCompletely(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	require := require.New(t)

	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	cfg.TotalBlobPoolLimit = 2 // tight limit: one 2-blob txn fills it
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)

	// Fund one account
	var addr [20]byte
	addr[0] = 1
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:       0,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	// Step 1: Add a pre-Osaka-shape blob txn (2 blobs, 2 proofs — one per blob).
	blobTxn := makeBlobTxn()
	blobTxn.IDHash[0] = 33
	blobTxn.Nonce = 0
	{
		var txnSlots TxnSlots
		txnSlots.Append(&blobTxn, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		require.Equal(txpoolcfg.Success, reasons[0], reasons[0].String())
	}

	// Sanity: pool has 2 blobs and 1 pending txn.
	require.Equal(uint64(2), pool.totalBlobsInPool.Load())
	require.Equal(1, pool.pending.Len())

	// Step 2: Simulate Osaka activation.
	pool.isPostOsaka.Store(true)

	// Step 3: Trigger best() via PeekBest — it will detect the proof-shape mismatch
	// and should fully discard the pre-Osaka txn.
	var txnsRlp TxnsRlp
	_, err = pool.PeekBest(ctx, 10, &txnsRlp, 0, math.MaxUint64, math.MaxUint64, math.MaxInt)
	require.NoError(err)

	// Step 4: Verify the pre-Osaka txn was fully discarded.
	require.Equal(0, pool.pending.Len(), "txn must be removed from pending")
	require.Equal(uint64(0), pool.totalBlobsInPool.Load(), "blob counter must be decremented to 0")
	_, inByHash := pool.byHash[string(blobTxn.IDHash[:])]
	require.False(inByHash, "txn must be removed from byHash")

	// Step 5: A valid Osaka-shaped blob txn must now be admittable (not rejected
	// with BlobPoolOverflow), proving the counters were properly cleaned up.
	chainID := uint256.MustFromBig(testforks.Forks["Cancun"].ChainID)
	osakaRlp := makeWrappedBlobTxnRlpWithCellProofs(t, chainID, 2)
	parseCtx := NewTxnParseContext(*chainID)
	parseCtx.WithSender(false)
	var osakaSlot TxnSlot
	_, err = parseCtx.ParseTransaction(osakaRlp, 0, &osakaSlot, nil, false, true, nil)
	require.NoError(err)
	osakaSlot.IDHash[0] = 34
	{
		var txnSlots TxnSlots
		txnSlots.Append(&osakaSlot, addr[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		require.Equal(txpoolcfg.Success, reasons[0], reasons[0].String())
	}
}

func TestWrappedSixBlobTxnExceedsRlpLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	require := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := make(chan Announcements, 1)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Osaka"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)

	chainID := uint256.MustFromBig(testforks.Forks["Osaka"].ChainID)
	rawTxn := makeWrappedBlobTxnRlpWithCellProofs(t, chainID, params.MaxBlobsPerTxn)

	parseCtx := NewTxnParseContext(*chainID)
	parseCtx.WithSender(false)
	parseCtx.ValidateRLP(pool.ValidateSerializedTxn)

	var slot TxnSlot
	_, err = parseCtx.ParseTransaction(rawTxn, 0, &slot, nil, false, true, nil)
	require.NoError(err)
}

func TestGetBlobs(t *testing.T) {
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)
	pool.blockGasLimit.Store(30000000)
	var stateVersionID uint64 = 0

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:       stateVersionID,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	var addr [20]byte

	// Add 1 eth to the user account, as a part of change
	acc := accounts3.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)

	for i := 0; i < 11; i++ {
		addr[0] = uint8(i + 1)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
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
	blobTxn.Txn.(*types.BlobTx).GasLimit = 50000
	txnSlots.Append(&blobTxn, addr[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txnSlots)
	require.NoError(err)
	for _, reason := range reasons {
		assert.Equal(txpoolcfg.Success, reason, reason.String())
	}
	blobHashes = append(blobHashes, blobTxn.GetBlobHashes()...)

	blobBundles := pool.GetBlobs(blobHashes)
	require.Equal(len(blobBundles), len(blobHashes))
	blobs := make([][]byte, 0, len(blobBundles))
	proofs := make([]goethkzg.KZGProof, 0, len(blobBundles))
	for _, bb := range blobBundles {
		blobs = append(blobs, bb.Blob)
		proofs = append(proofs, bb.Proofs...)
	}
	require.Equal(len(proofs), len(blobHashes))
	blob0, _, proofs0 := blobTxn.BlobBundle(0)
	blob1, _, proofs1 := blobTxn.BlobBundle(1)
	assert.Equal(blob0, blobs[0])
	assert.Equal(blob1, blobs[1])
	assert.Equal(proofs0[0], proofs[0])
	assert.Equal(proofs1[0], proofs[1])
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
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
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 1,
	}
	v := accounts3.SerialiseV3(&acc)
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       50_000,
		ChangeBatch: []*remoteproto.StateChange{
			{BlockHeight: 0, BlockHash: h1},
		},
	}
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    v,
	})
	err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
	require.NoError(err)

	var txnSlots TxnSlots
	txnSlot1 := newTestTxnSlot(3, 0, 300000, 300000, 100_000)
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
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotEqual(pool, nil)

	// Start the transaction pool
	err = pool.start(ctx)
	require.NoError(err)

	// Set up initial blockchain state
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1000000,
		ChangeBatch: []*remoteproto.StateChange{
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
			CodeHash:    accounts.EmptyCodeHash,
			Incarnation: 1,
		}
		v := accounts3.SerialiseV3(&acc)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
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
		addr[0] = uint8(i%100 + 1)                                          // Use one of our test accounts
		txnSlot := newTestTxnSlot(uint64(i/100), 0, 300000, 300000, 100000) // Different nonce for each account
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

// TestZombieQueuedEviction verifies that queued transactions whose nonce is so far ahead of
// the sender's on-chain nonce that they can never become pending are evicted from the pool.
// This covers Bug #2: "zombie" queued txns on Gnosis Chain (e.g. on-chain nonce=281 but
// queued nonce=16814, a gap of 16,533 that can never be filled).
func TestZombieQueuedEviction(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	db := memdb.NewTestPoolDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := txpoolcfg.DefaultConfig
	cfg.MaxNonceGap = 64 // explicit, same as default
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(err)
	require.NotNil(pool)

	pendingBaseFee := uint64(200_000)
	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	var senderAddr [20]byte
	senderAddr[0] = 0x42

	// Set sender's on-chain nonce = 5
	acc := accounts3.Account{
		Nonce:       5,
		Balance:     *uint256.NewInt(1 * common.Ether),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 0,
	}
	v := accounts3.SerialiseV3(&acc)
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      0,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1_000_000,
		ChangeBatch: []*remoteproto.StateChange{
			{
				BlockHeight: 0,
				BlockHash:   h1,
				Changes: []*remoteproto.AccountChange{
					{
						Action:  remoteproto.Action_UPSERT,
						Address: gointerfaces.ConvertAddressToH160(senderAddr),
						Data:    v,
					},
				},
			},
		},
	}
	require.NoError(pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	t.Run("zombie tx with impossible nonce gap is evicted", func(t *testing.T) {
		// nonce = 5 + 64 + 1 = 70, gap=65 > MaxNonceGap(64)
		zombieNonce := uint64(5 + cfg.MaxNonceGap + 1)
		var txnSlots TxnSlots
		slot := newTestTxnSlot(zombieNonce, 0, 300_000, 300_000, 100_000)
		slot.IDHash[0] = 0xAA
		txnSlots.Append(slot, senderAddr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		require.Len(reasons, 1)
		assert.Equal(txpoolcfg.NonceTooDistant, reasons[0],
			"zombie tx (nonce gap %d > MaxNonceGap %d) should be evicted with NonceTooDistant",
			zombieNonce-5, cfg.MaxNonceGap)

		_, _, queued := pool.CountContent()
		assert.Equal(0, queued, "queued pool should be empty after zombie eviction")
	})

	t.Run("tx at exactly MaxNonceGap boundary is kept", func(t *testing.T) {
		// nonce = 5 + 64 = 69, gap=64 == MaxNonceGap (NOT evicted)
		boundaryNonce := uint64(5 + cfg.MaxNonceGap)
		var txnSlots TxnSlots
		slot := newTestTxnSlot(boundaryNonce, 0, 300_000, 300_000, 100_000)
		slot.IDHash[0] = 0xBB
		txnSlots.Append(slot, senderAddr[:], true)

		reasons, err := pool.AddLocalTxns(ctx, txnSlots)
		require.NoError(err)
		require.Len(reasons, 1)
		// gap = boundaryNonce - noGapsNonce. noGapsNonce=5 (no consecutive txns).
		// 64 is NOT > 64, so the tx should be kept (in queued due to nonce gap).
		assert.Equal(txpoolcfg.Success, reasons[0],
			"tx at exactly MaxNonceGap boundary (gap=%d) should be accepted", cfg.MaxNonceGap)
	})

	t.Run("consecutive txns beyond MaxNonceGap are kept", func(t *testing.T) {
		// If there are consecutive txns 5, 6, 7, ..., 5+MaxNonceGap+5, they should all be kept
		// because the gap from noGapsNonce is always 0 for consecutive txns.
		var txnSlots TxnSlots
		baseNonce := uint64(5)
		// Clear the pool first by using a fresh pool
		ch2 := make(chan Announcements, 100)
		coreDB2 := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
		db2 := memdb.NewTestPoolDB(t)
		cfg2 := txpoolcfg.DefaultConfig
		cfg2.MaxNonceGap = 10 // small gap for this test
		pool2, err := New(ctx, ch2, db2, coreDB2, cfg2, kvcache.New(kvcache.DefaultCoherentConfig),
			chain.TestChainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
		require.NoError(err)

		acc2 := accounts3.Account{
			Nonce:    baseNonce,
			Balance:  *uint256.NewInt(10 * common.Ether),
			CodeHash: accounts.EmptyCodeHash,
		}
		v2 := accounts3.SerialiseV3(&acc2)
		var addr2 [20]byte
		addr2[0] = 0x99
		change2 := &remoteproto.StateChangeBatch{
			StateVersionId:      0,
			PendingBlockBaseFee: pendingBaseFee,
			BlockGasLimit:       1_000_000,
			ChangeBatch: []*remoteproto.StateChange{
				{
					BlockHeight: 0,
					BlockHash:   gointerfaces.ConvertHashToH256([32]byte{1}),
					Changes: []*remoteproto.AccountChange{
						{Action: remoteproto.Action_UPSERT, Address: gointerfaces.ConvertAddressToH160(addr2), Data: v2},
					},
				},
			},
		}
		require.NoError(pool2.OnNewBlock(ctx, change2, TxnSlots{}, TxnSlots{}, TxnSlots{}))

		// Add consecutive txns: nonces 5, 6, 7, ..., 5+MaxNonceGap+5 = 20
		count := int(cfg2.MaxNonceGap + 5 + 1)
		for i := 0; i < count; i++ {
			txnSlots.Txns = nil
			txnSlots.Senders = txnSlots.Senders[:0]
			txnSlots.IsLocal = txnSlots.IsLocal[:0]
			slot := newTestTxnSlot(baseNonce+uint64(i), 0, 300_000, 300_000, 100_000)
			slot.IDHash[0] = uint8(0xC0 + i)
			txnSlots.Append(slot, addr2[:], true)
			reasons, err := pool2.AddLocalTxns(ctx, txnSlots)
			require.NoError(err)
			assert.Equal(txpoolcfg.Success, reasons[0],
				"consecutive tx nonce=%d should not be evicted (gap from noGapsNonce is 0)", baseNonce+uint64(i))
		}
		// All consecutive txns (no nonce gaps) should be accepted and promoted to pending.
		// The key check is that NONE were evicted with NonceTooDistant — verified above per-tx.
		pending2, _, queued2 := pool2.CountContent()
		assert.Equal(0, queued2, "no consecutive txns should be zombie-evicted (queued should be drained to pending)")
		assert.Equal(count, pending2, "all consecutive txns should be pending (no gaps, sufficient balance)")
	})
}

// TestStalePendingEvictionViaMineNonce verifies that when the execution layer
// correctly emits a state-diff UPSERT for an AuRa/Gnosis system-transaction
// sender (fixed in exec3_serial.go by passing the accumulator to the block-end
// stateWriter), the txpool evicts the now-stale pending transactions.
//
// Scenario:
//  1. addr1 has two pending txns: T1 (nonce=0) and T2 (nonce=1).
//  2. The block mines T1 from the pool AND an AuRa system tx at nonce=1,
//     advancing the on-chain nonce to 2. Only T1 appears in minedTxns.
//  3. The EL (after the exec3_serial.go fix) emits addr1 with nonce=2 in
//     stateChanges. The pool receives this via OnNewBlock.
//  4. removeMined removes T1; onSenderStateChange(nonce=2) evicts T2 (nonce=1).
func TestStalePendingEvictionViaMineNonce(t *testing.T) {
	asrt := assert.New(t)
	req := require.New(t)
	logger := log.New()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := make(chan Announcements, 100)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	cfg := txpoolcfg.DefaultConfig

	// DummyCache reads directly from the DB — avoids coherence-version coupling.
	pool, err := New(ctx, ch, nil, coreDB, cfg, kvcache.NewDummy(), chain.TestChainConfig, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
	req.NoError(err)
	req.NotNil(pool)

	var addr1 [20]byte
	addr1[0] = 1
	h0 := gointerfaces.ConvertHashToH256([32]byte{})

	// writeAccount writes addr1 to coreDB at the given nonce so that senders.info
	// (which reads from the DB when using DummyCache) returns the expected value.
	writeAccount := func(nonce, txNum uint64) {
		tx, werr := coreDB.BeginTemporalRw(ctx)
		req.NoError(werr)
		defer tx.Rollback()
		sd, werr := execctx.NewSharedDomains(ctx, tx, logger)
		req.NoError(werr)
		a := accounts3.Account{
			Nonce: nonce, Balance: *uint256.NewInt(1 * common.Ether),
			CodeHash: accounts.EmptyCodeHash, Incarnation: 1,
		}
		req.NoError(sd.DomainPut(kv.AccountsDomain, tx, addr1[:], accounts3.SerialiseV3(&a), txNum, nil))
		req.NoError(sd.Flush(ctx, tx))
		sd.Close()
		req.NoError(tx.Commit())
	}

	serialiseAcc := func(nonce uint64) []byte {
		a := accounts3.Account{
			Nonce: nonce, Balance: *uint256.NewInt(1 * common.Ether),
			CodeHash: accounts.EmptyCodeHash, Incarnation: 1,
		}
		return accounts3.SerialiseV3(&a)
	}

	// ── Step 1: write addr1 nonce=0 to DB and bootstrap pool ─────────────────
	writeAccount(0, 0)
	initChange := &remoteproto.StateChangeBatch{
		StateVersionId: 0, PendingBlockBaseFee: 200_000, BlockGasLimit: 1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h0}},
	}
	initChange.ChangeBatch[0].Changes = append(initChange.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr1),
		Data:    serialiseAcc(0),
	})
	req.NoError(pool.OnNewBlock(ctx, initChange, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	// ── Step 2: add T1 (nonce=0) and T2 (nonce=1) to pending ────────────────
	T1 := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	T1.IDHash[0] = 1
	T2 := newTestTxnSlot(1, 0, 300_000, 300_000, 100_000)
	T2.IDHash[0] = 2
	var slots TxnSlots
	slots.Append(T1, addr1[:], true)
	slots.Append(T2, addr1[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	req.NoError(err)
	for _, r := range reasons {
		asrt.Equal(txpoolcfg.Success, r, r.String())
	}
	pending, _, _ := pool.CountContent()
	asrt.Equal(2, pending, "both T1 and T2 should be in pending")

	// ── Step 3: advance DB nonce to 2 (T1 mined + AuRa system tx at nonce=1) ─
	writeAccount(2, 1)

	// ── Step 4: OnNewBlock with the correct stateChanges from the fixed EL ───
	// The exec3_serial.go fix ensures addr1 appears in stateChanges with
	// nonce=2 because the block-end stateWriter now carries the accumulator.
	h1 := gointerfaces.ConvertHashToH256([32]byte{1})
	blockChange := &remoteproto.StateChangeBatch{
		StateVersionId: 1, PendingBlockBaseFee: 200_000, BlockGasLimit: 1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{BlockHeight: 1, BlockHash: h1}},
	}
	blockChange.ChangeBatch[0].Changes = append(blockChange.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr1),
		Data:    serialiseAcc(2), // EL now correctly emits nonce=2
	})

	minedT1 := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	minedT1.IDHash[0] = 1
	var minedSlots TxnSlots
	minedSlots.Append(minedT1, addr1[:], true)

	req.NoError(pool.OnNewBlock(ctx, blockChange, TxnSlots{}, TxnSlots{}, minedSlots))

	// ── Step 5: T1 removed by removeMined; T2 evicted by onSenderStateChange ─
	// senderNonce=2 (from DB) > T2.Nonce=1 → NonceTooLow.
	pending, _, queued := pool.CountContent()
	asrt.Equal(0, pending, "T2 must be evicted: on-chain nonce=2 > T2.nonce=1")
	asrt.Equal(0, queued, "no queued txns expected")
}
