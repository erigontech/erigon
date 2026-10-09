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
	"net"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/remotedb"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type authorizationReadProbe struct {
	types.Transaction
	pool       *TxPool
	reads      int
	onUnlocked func()
}

func (p *authorizationReadProbe) GetAuthorizations() []types.Authorization {
	p.reads++
	if p.onUnlocked != nil && p.pool.lock.TryLock() {
		p.pool.lock.Unlock()
		callback := p.onUnlocked
		p.onUnlocked = nil
		callback()
	}
	return p.Transaction.GetAuthorizations()
}

func TestAuthorizationRecoveryRemoteDB(t *testing.T) {
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	remoteproto.RegisterKVServer(server, remotedbserver.NewKvServer(ctx, coreDB, nil, nil, pool.logger))
	t.Cleanup(server.Stop)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	remoteDB, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), pool.logger, remoteproto.NewKVClient(conn)).Open()
	require.NoError(t, err)
	pool._chainDB = remoteDB

	for nonce := range uint64(2) {
		txn := newTestSetCodeTxnSlot(nonce, 0, 1, 2, 100_000)
		txn.IDHash[0] = byte(nonce + 1)
		var slots TxnSlots
		slots.Append(txn, sender[:], true)
		require.NotPanics(t, func() {
			reasons, err := pool.AddLocalTxns(ctx, slots)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
		})
	}
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
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, pool: pool, onUnlocked: func() {
		unlocked = true
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
	require.True(t, known, "an invalid self-authorization nonce must be remembered before the next parse")
}

func TestAuthorizationRecoveryRevalidatesBalance(t *testing.T) {
	ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, 0)
	require.NoError(t, err)
	txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash[0] = 1
	txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, pool: pool, onUnlocked: func() {
		// Apply the balance change after prechecks and before admission resumes.
		account := accounts.Account{CodeHash: accounts.EmptyCodeHash}
		encoded := accounts.SerialiseV3(&account)
		writeTestSenderState(t, ctx, coreDB, pool.logger, sender, encoded, 1)
		change := &remoteproto.StateChangeBatch{
			StateVersionId:      1,
			PendingBlockBaseFee: 1,
			BlockGasLimit:       1_000_000,
			ChangeBatch: []*remoteproto.StateChange{{
				BlockHeight: 1,
				BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{1}),
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(sender),
					Data:    encoded,
				}},
			}},
		}
		require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	}}
	var slots TxnSlots
	slots.Append(txn, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.InsufficientFunds}, reasons)
	require.Equal(t, []AuthAndNonce{{crypto.PubkeyToAddress(key.PublicKey), 0}}, txn.AuthAndNonces,
		"the transaction must pass prechecks and complete recovery")
	require.Empty(t, pool.byHash)
	require.Empty(t, pool.auths)
}

type stateViewProbe struct {
	kvcache.Cache
	onView func()
}

func (c *stateViewProbe) View(ctx context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	view, err := c.Cache.View(ctx, tx)
	if err == nil && c.onView != nil {
		callback := c.onView
		c.onView = nil
		callback()
	}
	return view, err
}

func TestAuthorizationRecoveryReopensStaleView(t *testing.T) {
	for _, tc := range []struct {
		name    string
		nonce   uint64
		balance uint64
		reason  txpoolcfg.DiscardReason
	}{
		{"balance", 0, 0, txpoolcfg.InsufficientFunds},
		{"nonce", 1, common.Ether, txpoolcfg.NonceTooLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			cache := &stateViewProbe{Cache: pool._stateCache}
			pool._stateCache = cache
			height := pool.lastSeenBlock.Load()
			txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000)
			txn.IDHash[0] = 1
			changed := false
			txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, pool: pool, onUnlocked: func() {
				cache.onView = func() {
					require.True(t, pool.lock.TryLock(), "opening a state view must not hold the pool lock")
					pool.lock.Unlock()
					account := accounts.Account{Nonce: tc.nonce, Balance: *uint256.NewInt(tc.balance), CodeHash: accounts.EmptyCodeHash}
					encoded := accounts.SerialiseV3(&account)
					writeTestSenderState(t, ctx, coreDB, pool.logger, sender, encoded, 1)
					change := &remoteproto.StateChangeBatch{
						StateVersionId:      1,
						PendingBlockBaseFee: 1,
						BlockGasLimit:       1_000_000,
						ChangeBatch: []*remoteproto.StateChange{{
							// State changes at the same height must also invalidate the read view.
							BlockHeight: height,
							BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{1}),
							Changes: []*remoteproto.AccountChange{{
								Action:  remoteproto.Action_UPSERT,
								Address: gointerfaces.ConvertAddressToH160(sender),
								Data:    encoded,
							}},
						}},
					}
					require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
					changed = true
				}
			}}
			var slots TxnSlots
			slots.Append(txn, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, slots)
			require.NoError(t, err)
			require.True(t, changed, "the block update must run after the final view opens")
			require.Equal(t, height, pool.lastSeenBlock.Load())
			require.NotNil(t, txn.AuthAndNonces, "recovery must complete before the block update")
			require.Equal(t, []txpoolcfg.DiscardReason{tc.reason}, reasons)
			require.Empty(t, pool.byHash)
		})
	}
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
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, pool: pool, onUnlocked: func() {
		unlocked = true
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
	ctx, pool, db, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
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
	dbTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	known, err := pool.IdHashKnown(dbTx, rejected.IDHash[:])
	require.NoError(t, err)
	require.False(t, known, "a temporary unwind rejection must remain retryable")
}

func TestRejectedUnwindKeepsPooledTransaction(t *testing.T) {
	for _, kind := range []string{"regular", "blob"} {
		t.Run(kind, func(t *testing.T) {
			ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			pooled := newTestTxnSlot(0, 0, 10, 10, 100_000)
			pooled.IDHash[0] = 1
			var slots TxnSlots
			slots.Append(pooled, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, slots)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			owner := pool.byHash[string(pooled.IDHash[:])]

			rejected := newTestTxnSlot(0, 0, 10, 10, 100_000)
			if kind == "blob" {
				rejected = newTestBlobTxnSlot(0, 0, 10, 10, 100_000)
				rejected.Txn.(*types.BlobTx).BlobVersionedHashes = []common.Hash{{1}}
			}
			rejected.IDHash[0] = 2
			var unwind TxnSlots
			unwind.Append(rejected, sender[:], false)
			err = pool.withLockedState(ctx, &unwind, func(view kvcache.CacheView) error {
				_, err := pool.addTxnsOnNewBlock(1, view, &remoteproto.StateChangeBatch{}, pool.senders, unwind,
					pool.pendingBaseFee.Load(), pool.blockGasLimit.Load(), pool.logger)
				return err
			})
			require.NoError(t, err)
			require.Zero(t, pool.totalBlobsInPool.Load())
			require.Same(t, owner, pool.all.get(pooled.SenderID, pooled.Nonce))
			require.Same(t, owner, pool.byHash[string(pooled.IDHash[:])])
			require.Equal(t, 1, pool.pending.Len())
			require.NotContains(t, pool.byHash, string(rejected.IDHash[:]))
		})
	}
}

func TestAdmissionRejectionsRemainRetryable(t *testing.T) {
	for _, reason := range []txpoolcfg.DiscardReason{txpoolcfg.ErrAuthorityReserved, txpoolcfg.NotReplaced} {
		t.Run(reason.String(), func(t *testing.T) {
			ctx, pool, db, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			pooled := newTestTxnSlot(0, 0, 10, 10, 100_000)
			retry := newTestTxnSlot(0, 0, 10, 10, 100_000)
			if reason == txpoolcfg.ErrAuthorityReserved {
				pooled = newTestSetCodeTxnSlot(0, 0, 10, 10, 100_000)
				retry = newTestSetCodeTxnSlot(1, 0, 10, 10, 100_000)
				pooled.AuthAndNonces = []AuthAndNonce{{common.Address{2}, 0}}
				retry.AuthAndNonces = pooled.AuthAndNonces
			}
			pooled.IDHash[0], retry.IDHash[0] = 1, 2
			var slots TxnSlots
			slots.Append(pooled, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, slots)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

			var retrySlots TxnSlots
			retrySlots.Append(retry, sender[:], true)
			reasons, err = pool.AddLocalTxns(ctx, retrySlots)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{reason}, reasons)
			require.NotContains(t, pool.byHash, string(retry.IDHash[:]))

			pool.lock.Lock()
			owner := pool.byHash[string(pooled.IDHash[:])]
			pool.removeFromSubPool(owner, "test")
			pool.discardLocked(owner, txpoolcfg.Mined)
			pool.lock.Unlock()

			dbTx, err := db.BeginRo(ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			known, err := pool.IdHashKnown(dbTx, retry.IDHash[:])
			require.NoError(t, err)
			require.False(t, known, "a temporary rejection must not prevent resubmission")
			reasons, err = pool.AddLocalTxns(ctx, retrySlots)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			require.Contains(t, pool.byHash, string(retry.IDHash[:]))
		})
	}
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
	txn.Txn = &authorizationReadProbe{Transaction: txn.Txn, pool: pool, onUnlocked: func() {
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

func (db *failSecondTemporalReadDB) ViewTemporal(ctx context.Context, f func(kv.TemporalTx) error) error {
	db.reads++
	if db.reads == 2 {
		return db.err
	}
	return db.TemporalRoDB.ViewTemporal(ctx, f)
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
			probe := &authorizationReadProbe{Transaction: txn.Txn}
			txn.Txn = probe
			pool.recoverAuthorizations(txn)
			pool.recoverAuthorizations(txn)
			require.Equal(t, 1, probe.reads, "authorization recovery must not repeat for the same transaction")
		})
	}
}
