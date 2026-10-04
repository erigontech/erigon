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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type authorizationReadProbe struct {
	types.Transaction
	reads    int
	onSecond func()
}

func (p *authorizationReadProbe) GetAuthorizations() []types.Authorization {
	p.reads++
	if p.reads == 2 {
		p.onSecond()
	}
	return p.Transaction.GetAuthorizations()
}

func TestLocalAuthorizationRecoveryReleasesPoolLock(t *testing.T) {
	ctx, pool, db, _, _ := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 0, 0, 100_000)
	txn.IDHash[0] = 1
	txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	var unlocked bool
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, onSecond: func() {
		// Validation reads the list length first; recovery reads its entries next.
		unlocked = pool.lock.TryLock()
		if unlocked {
			pool.lock.Unlock()
		}
	}}
	var slots TxnSlots
	slots.Append(txn, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooLow}, reasons)
	require.True(t, unlocked, "authorization recovery must not hold the pool lock")
	dbTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	known, err := pool.IdHashKnown(dbTx, txn.IDHash[:])
	require.NoError(t, err)
	require.True(t, known, "late rejections must be remembered before the next parse")
}

func TestRemoteAuthorizationRecoveryReleasesPoolLock(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.started.Store(true)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash[0] = 1
	txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	var unlocked bool
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, onSecond: func() {
		unlocked = pool.lock.TryLock()
		if unlocked {
			pool.lock.Unlock()
		}
	}}
	var slots TxnSlots
	slots.Append(txn, sender[:], false)
	pool.AddRemoteTxns(ctx, slots, nil, nil)
	require.Empty(t, txn.AuthAndNonces, "enqueue must defer authorization recovery to batch processing")
	require.NoError(t, pool.processRemoteTxns(ctx))
	require.Contains(t, pool.byHash, string(txn.IDHash[:]))
	require.True(t, unlocked, "authorization recovery must not hold the pool lock")
}

func TestRejectedUnwindKeepsAuthorityReservation(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(key.PublicKey)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	pooled := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
	pooled.IDHash[0] = 1
	pooled.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	var slots TxnSlots
	slots.Append(pooled, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	reservation := AuthAndNonce{authority, 0}
	owner := pool.auths[reservation]
	require.NotNil(t, owner)

	rejected := newTestSetCodeTxnSlot(1, 0, 1, 2, 100_000)
	rejected.IDHash[0] = 2
	rejected.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	var unwind TxnSlots
	unwind.Append(rejected, sender[:], false)
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 1,
		BlockGasLimit:       1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 1,
			BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{1}),
		}},
	}
	require.NoError(t, pool.OnNewBlock(ctx, change, unwind, TxnSlots{}, TxnSlots{}))
	require.Same(t, owner, pool.auths[reservation])
	require.Equal(t, pooled.AuthAndNonces, rejected.AuthAndNonces)
}

func TestSetCodeAuthorizationChainIDs(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	keyA, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyB, err := crypto.GenerateKey()
	require.NoError(t, err)
	authA, err := types.SignAuthorization(keyA, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	authB, err := types.SignAuthorization(keyB, uint256.Int{}, common.Address{2}, 3)
	require.NoError(t, err)
	foreign, err := types.SignAuthorization(keyA, *uint256.NewInt(2), common.Address{2}, 1)
	require.NoError(t, err)
	large, err := types.SignAuthorization(keyA, *new(uint256.Int).SetAllOne(), common.Address{2}, 2)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 200_000)
	txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{authA, {}, foreign, large, authB}
	var slots TxnSlots
	slots.Append(txn, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	expected := []AuthAndNonce{{crypto.PubkeyToAddress(keyA.PublicKey), 0}, {crypto.PubkeyToAddress(keyB.PublicKey), 3}}
	require.Equal(t, expected, txn.AuthAndNonces)
	require.Len(t, pool.auths, len(expected))
	for _, a := range expected {
		require.Same(t, pool.byHash[string(txn.IDHash[:])], pool.auths[a])
	}
}

func TestRemoteAuthorizationRecoveryPreservesQueue(t *testing.T) {
	ctx, pool, db, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.started.Store(true)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash[0] = 1
	txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	later := newTestTxnSlot(1, 0, 1, 2, 21_000)
	later.IDHash[0] = 2
	peer := PeerID(gointerfaces.ConvertHashToH512([64]byte{2}))
	dbTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, onSecond: func() {
		known, err := pool.IdHashKnown(dbTx, txn.IDHash[:])
		require.NoError(t, err)
		require.True(t, known, "a transaction being recovered must remain known")
		var arrivals TxnSlots
		arrivals.Append(txn, sender[:], false)
		arrivals.Append(later, sender[:], false)
		pool.AddRemoteTxns(ctx, arrivals, peer, nil)
	}}
	var slots TxnSlots
	slots.Append(txn, sender[:], false)
	pool.AddRemoteTxns(ctx, slots, nil, nil)
	require.NoError(t, pool.processRemoteTxns(ctx))
	require.Contains(t, pool.byHash, string(txn.IDHash[:]))
	require.NotContains(t, pool.byHash, string(later.IDHash[:]))
	require.Equal(t, []*TxnSlot{later}, pool.unprocessedRemoteTxns.Txns)
	require.Equal(t, map[string]*TxnSlot{string(later.IDHash[:]): later}, pool.unprocessedRemoteByHash)
	require.Len(t, pool.unprocessedRemotePeers, 1)
	require.Equal(t, peer, pool.unprocessedRemotePeers[0].peerID)
	require.True(t, pool.hasUnprocessedRemoteTxns.Load())
	require.NoError(t, pool.processRemoteTxns(ctx))
	require.Contains(t, pool.byHash, string(later.IDHash[:]))
	require.Empty(t, pool.unprocessedRemoteTxns.Txns)
	require.Empty(t, pool.unprocessedRemoteByHash)
	require.Empty(t, pool.unprocessedRemotePeers)
	require.False(t, pool.hasUnprocessedRemoteTxns.Load())
}

type failSecondTemporalReadDB struct {
	kv.TemporalRoDB
	reads int
	err   error
}

func (db *failSecondTemporalReadDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	db.reads++
	if db.reads == 2 {
		return nil, db.err
	}
	return db.TemporalRoDB.BeginTemporalRo(ctx)
}

func TestRemoteAuthorizationRecoveryKeepsQueueOnError(t *testing.T) {
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	pool.started.Store(true)
	readErr := errors.New("state read failed")
	pool._chainDB = &failSecondTemporalReadDB{TemporalRoDB: coreDB, err: readErr}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash[0] = 1
	txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	var slots TxnSlots
	slots.Append(txn, sender[:], false)
	peer := PeerID(gointerfaces.ConvertHashToH512([64]byte{1}))
	sentry := sentryproto.NewMockSentryClient(gomock.NewController(t))
	pool.AddRemoteTxns(ctx, slots, peer, sentry)

	require.ErrorIs(t, pool.processRemoteTxns(ctx), readErr)
	require.Equal(t, []AuthAndNonce{{crypto.PubkeyToAddress(key.PublicKey), 0}}, txn.AuthAndNonces,
		"recovery must finish before the failed state read")
	require.Empty(t, pool.byHash)
	require.Empty(t, pool.auths)
	require.Equal(t, slots, *pool.unprocessedRemoteTxns)
	require.Equal(t, map[string]*TxnSlot{string(txn.IDHash[:]): txn}, pool.unprocessedRemoteByHash)
	require.Equal(t, []remoteSource{{peerID: peer, sentry: sentry}}, pool.unprocessedRemotePeers)
	require.True(t, pool.hasUnprocessedRemoteTxns.Load())

	require.NoError(t, pool.processRemoteTxns(ctx))
	require.Contains(t, pool.byHash, string(txn.IDHash[:]))
	require.Same(t, pool.byHash[string(txn.IDHash[:])], pool.auths[txn.AuthAndNonces[0]])
	require.Empty(t, pool.unprocessedRemoteTxns.Txns)
	require.Empty(t, pool.unprocessedRemoteTxns.Senders)
	require.Empty(t, pool.unprocessedRemoteTxns.IsLocal)
	require.Empty(t, pool.unprocessedRemoteByHash)
	require.Empty(t, pool.unprocessedRemotePeers)
	require.False(t, pool.hasUnprocessedRemoteTxns.Load())
}

func TestAuthorizationRecoveryIsCached(t *testing.T) {
	pool := &TxPool{chainID: *uint256.NewInt(1)}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		auth types.Authorization
	}{
		{"valid", auth},
		{"invalid", types.Authorization{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
			txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{tc.auth}
			probe := &authorizationReadProbe{Transaction: txn.Txn, onSecond: func() {
				t.Error("authorization recovery must not repeat for the same transaction")
			}}
			txn.Txn = probe
			pool.recoverAuthorizations(txn)
			pool.recoverAuthorizations(txn)
		})
	}
}
