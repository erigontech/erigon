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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func p2pRetryForTest(t *testing.T, pool *TxPool, hashes Hashes, seen uint64) (uint64, bool) {
	t.Helper()
	revision, retry, err := pool.P2PRetry(t.Context(), hashes, seen)
	require.NoError(t, err)
	return revision, retry
}

func updateP2PRetryNonce(t *testing.T, pool *TxPool, sender common.Address, nonce, block uint64) {
	t.Helper()
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 1,
		BlockGasLimit:       1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: block,
			BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{}),
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(sender),
				Data: accounts.SerialiseV3(&accounts.Account{
					Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
				}),
			}},
		}},
	}
	require.NoError(t, pool.OnNewBlock(t.Context(), change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
}

func TestP2PRetryPrecheckHistoryPreservesKnownHashPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		feeCap uint64
		reason txpoolcfg.DiscardReason
	}{
		{"underpriced", 0, txpoolcfg.UnderPriced},
		{"insufficient funds", 1_000_000_000_000_000, txpoolcfg.InsufficientFunds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, db, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			require.NoError(t, pool.start(ctx))
			seen, _ := p2pRetryForTest(t, pool, nil, 0)
			txn := newTestTxnSlot(0, 0, 0, tc.feeCap, 21_000)
			txn.IDHash[0] = 1
			var txns TxnSlots
			txns.Append(txn, sender[:], false)
			pool.AddRemoteTxns(ctx, txns, nil, nil)
			require.NoError(t, pool.processRemoteTxns(ctx))
			_, retry := p2pRetryForTest(t, pool, txn.IDHash[:], seen)
			require.False(t, retry)
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, txn.IDHash[:])
				require.NoError(t, err)
				require.False(t, known)
				return nil
			}))
			updateP2PRetryNonce(t, pool, sender, 1, 1)
			_, retry = p2pRetryForTest(t, pool, txn.IDHash[:], seen)
			require.False(t, retry, "%s is not a nonce-gap retry", tc.reason)
			pool.discardReasonsLRU.Purge()
			_, retry = p2pRetryForTest(t, pool, txn.IDHash[:], seen)
			require.True(t, retry, "evicted rejection history must not become a permanent veto")
		})
	}
}

func TestP2PRetryNonceFrontierAdvancesWithoutNewBlock(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	require.NoError(t, pool.start(ctx))
	seen, _ := p2pRetryForTest(t, pool, nil, 0)
	target := newTestTxnSlot(66, 0, 1, 2, 21_000)
	target.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(target, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooDistant}, reasons)
	_, retry := p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.False(t, retry)
	for _, nonce := range []uint64{1, 0} {
		bridge := newTestTxnSlot(nonce, 0, 1, 2, 21_000)
		bridge.IDHash[0] = byte(nonce + 2)
		var lower TxnSlots
		lower.Append(bridge, sender[:], true)
		reasons, err = pool.AddLocalTxns(ctx, lower)
		require.NoError(t, err)
		require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
		if nonce == 1 {
			_, retry = p2pRetryForTest(t, pool, target.IDHash[:], seen)
			require.False(t, retry, "a transaction beyond the first gap does not advance the frontier")
		}
	}
	revision, retry := p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.True(t, retry)
	require.Greater(t, revision, seen)
	_, retry = p2pRetryForTest(t, pool, target.IDHash[:], revision)
	require.False(t, retry, "one packet cannot reuse the same frontier advance")
	_, retry = p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.True(t, retry, "another cached packet has its own retry opportunity")
	_, verdict := pool.AddRemoteTxns(ctx, txns, nil, nil)
	require.NotNil(t, verdict)
	_, verdict = pool.AddRemoteTxns(ctx, txns, nil, nil)
	require.Nil(t, verdict, "a duplicate does not own the queued packet verdict")
	_, retry = p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.False(t, retry, "queued transactions already have an admission attempt")
	require.NoError(t, pool.processRemoteTxns(ctx))
	_, retry = p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.False(t, retry, "accepted transactions must remain suppressed")
}

func TestP2PRetryRemembersReplacementRejection(t *testing.T) {
	ctx, pool, db, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	for _, hashByte := range []byte{1, 2} {
		txn := newTestTxnSlot(0, 0, 100, 100, 21_000)
		txn.IDHash[0] = hashByte
		var txns TxnSlots
		txns.Append(txn, sender[:], true)
		reasons, err := pool.AddLocalTxns(ctx, txns)
		require.NoError(t, err)
		if hashByte == 1 {
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
		} else {
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NotReplaced}, reasons)
			_, retry := p2pRetryForTest(t, pool, txn.IDHash[:], 0)
			require.False(t, retry)
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, txn.IDHash[:])
				require.NoError(t, err)
				require.False(t, known)
				return nil
			}))
		}
	}
}

func TestP2PRetryUsesNonceAfterSenderCleanup(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	require.NoError(t, pool.start(ctx))
	seen, _ := p2pRetryForTest(t, pool, nil, 0)
	target := newTestTxnSlot(65, 0, 1, 2, 21_000)
	target.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(target, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooDistant}, reasons)
	require.Empty(t, pool.senders.senderIDs)
	updateP2PRetryNonce(t, pool, sender, 0, 1)
	_, retry := p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.False(t, retry, "a new block with the same nonce does not advance the frontier")
	updateP2PRetryNonce(t, pool, sender, 1, 2)
	revision, retry := p2pRetryForTest(t, pool, target.IDHash[:], seen)
	require.True(t, retry)
	updateP2PRetryNonce(t, pool, sender, 1, 3)
	_, retry = p2pRetryForTest(t, pool, target.IDHash[:], revision)
	require.False(t, retry)
}

func TestP2PRetryBadSidecarPreservesHonestPacketProgress(t *testing.T) {
	ctx := t.Context()
	pool := seedBlobKZGTestPool(t, ctx)
	seen, _ := p2pRetryForTest(t, pool, nil, 0)
	sender := common.Address{1}
	good := makeBlobSlot(1, false)
	good.Nonce = 65
	good.Txn.(*types.BlobTx).Nonce = 65
	var txns TxnSlots
	txns.Append(&good, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooDistant}, reasons)
	bad := makeBlobSlot(1, true)
	bad.Nonce = 65
	bad.Txn.(*types.BlobTx).Nonce = 65
	var badTxns TxnSlots
	badTxns.Append(&bad, sender[:], true)
	reasons, err = pool.AddLocalTxns(ctx, badTxns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.UnmatchedBlobTxExt}, reasons)
	bridge := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	bridge.IDHash[0] = 2
	var lower TxnSlots
	lower.Append(bridge, sender[:], true)
	reasons, err = pool.AddLocalTxns(ctx, lower)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	revision, retry := p2pRetryForTest(t, pool, good.IDHash[:], seen)
	require.True(t, retry)
	_, retry = p2pRetryForTest(t, pool, good.IDHash[:], revision)
	require.False(t, retry, "the bad packet cannot reuse the same progress token")
	_, retry = p2pRetryForTest(t, pool, good.IDHash[:], seen)
	require.True(t, retry, "the good packet still has its own progress token")
}

func TestRemotePacketTracksSidecarFailure(t *testing.T) {
	ctx := t.Context()
	pool := seedBlobKZGTestPool(t, ctx)
	require.NoError(t, pool.start(ctx))
	bad := makeBlobSlot(1, true)
	valid := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	valid.IDHash[0] = 2
	var txns TxnSlots
	badSender, validSender := common.Address{1}, common.Address{2}
	txns.Append(&bad, badSender[:], false)
	txns.Append(valid, validSender[:], false)
	submitted, invalidSidecar := pool.AddRemoteTxns(ctx, txns, peerID, nil)
	require.Equal(t, append(append(Hashes{}, bad.IDHash[:]...), valid.IDHash[:]...), submitted)
	require.NotNil(t, invalidSidecar)
	reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.UnmatchedBlobTxExt, txpoolcfg.Success}, reasons)
	require.Empty(t, pool.unprocessedRemoteByHash)
	require.Empty(t, pool.byHash)
	require.True(t, invalidSidecar.invalidSidecar.Load(), "the cached packet must remember its own sidecar failure")
	var retryTxns TxnSlots
	retryTxns.Append(valid, validSender[:], false)
	submitted, honestSidecar := pool.AddRemoteTxns(ctx, retryTxns, peerID, nil)
	require.Equal(t, Hashes(valid.IDHash[:]), submitted)
	require.NoError(t, pool.processRemoteTxns(ctx))
	require.False(t, honestSidecar.invalidSidecar.Load(), "another packet must not inherit the earlier sidecar failure")
	require.Contains(t, pool.byHash, string(valid.IDHash[:]), "a fresh valid packet can still deliver the skipped transaction")
}

func TestP2PRetrySuppressesIgnoredRemoteTransactions(t *testing.T) {
	ctx, pool, db, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	require.NoError(t, pool.start(ctx))
	pool.cfg.NoGossip = true
	seen, _ := p2pRetryForTest(t, pool, nil, 0)
	txn := newTestTxnSlot(0, 0, 1, 2, 21_000)
	txn.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(txn, sender[:], false)
	pool.AddRemoteTxns(ctx, txns, peerID, nil)
	require.Empty(t, pool.unprocessedRemoteTxns.Txns)
	_, retry := p2pRetryForTest(t, pool, txn.IDHash[:], seen)
	require.False(t, retry, "ignored remote packets should remain deduplicated")
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		known, err := pool.IdHashKnown(tx, txn.IDHash[:])
		require.NoError(t, err)
		require.False(t, known)
		return nil
	}))
}

func TestCachedPacketRetriesNonceGapSkippedByAnotherPacket(t *testing.T) {
	ctx := t.Context()
	pool := seedBlobKZGTestPool(t, ctx)
	require.NoError(t, pool.start(ctx))
	sender := common.Address{1}
	good := makeBlobSlot(1, false)
	good.Nonce = 65
	good.Txn.(*types.BlobTx).Nonce = 65
	good.Txn.(*types.BlobTx).GasLimit = 100_000
	var goodTxns TxnSlots
	goodTxns.Append(&good, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, goodTxns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooDistant}, reasons)
	updateP2PRetryNonce(t, pool, sender, 65, 1)
	revision, retry := p2pRetryForTest(t, pool, good.IDHash[:], 0)
	require.True(t, retry)
	require.Greater(t, revision, uint64(0))
	bad := makeBlobSlot(2, true)
	bad.Txn.(*types.BlobTx).GasLimit = 100_000
	badSender := common.Address{2}
	var badTxns TxnSlots
	badTxns.Append(&bad, badSender[:], false)
	_, badFlag := pool.AddRemoteTxns(ctx, badTxns, peerID, nil)
	submitted, goodFlag := pool.AddRemoteTxns(ctx, goodTxns, peerID, nil)
	require.Equal(t, Hashes(good.IDHash[:]), submitted)
	entry := bodyCacheEntry{hashes: good.IDHash[:], revision: revision, result: goodFlag}
	reasons, err = pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.UnmatchedBlobTxExt, txpoolcfg.NonceTooDistant}, reasons)
	require.True(t, badFlag.invalidSidecar.Load())
	require.False(t, goodFlag.invalidSidecar.Load())
	require.True(t, goodFlag.incomplete.Load())
	require.Empty(t, pool.byHash)
	_, retry, err = entry.retry(ctx, pool)
	require.NoError(t, err)
	require.True(t, retry, "a packet skipped before validation has not used its nonce-progress opportunity")
}
