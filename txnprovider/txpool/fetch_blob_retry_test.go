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
	"crypto/ecdsa"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func newFetchBlobRetryFixture(t *testing.T, nonce uint64) (*TxPool, *types.BlobTxWrapper, []byte, []byte, *ecdsa.PrivateKey) {
	t.Helper()
	ctx := t.Context()
	pool := seedBlobKZGTestPool(t, ctx)
	require.NoError(t, pool.start(ctx))
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 200_000, PendingBlobFeePerGas: 100_000, BlockGasLimit: 30_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 1, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Changes: []*remoteproto.AccountChange{{
				Action: remoteproto.Action_UPSERT, Address: gointerfaces.ConvertAddressToH160(sender),
				Data: accounts.SerialiseV3(&accounts.Account{
					Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash,
				}),
			}},
		}},
	}
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	wrapper := &types.BlobTxWrapper{
		Tx: types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: nonce, To: &common.Address{9}, GasLimit: 100_000},
				ChainID:  pool.chainID, TipCap: *uint256.NewInt(300_000), FeeCap: *uint256.NewInt(300_000),
			},
			MaxFeePerBlobGas: *uint256.NewInt(200_000),
		},
		Blobs: make(types.Blobs, 1),
	}
	wrapper.Blobs[0][31] = 1
	commitments, hashes, proofs, err := wrapper.Blobs.ComputeCommitmentsAndProofs()
	require.NoError(t, err)
	wrapper.Commitments = commitments
	wrapper.Proofs = proofs
	wrapper.Tx.BlobVersionedHashes = hashes
	signed, err := types.SignTx(wrapper, *types.LatestSignerForChainID(&pool.chainID), key)
	require.NoError(t, err)
	good := signed.(*types.BlobTxWrapper)
	var goodWire, badWire bytes.Buffer
	require.NoError(t, good.MarshalBinaryWrapped(&goodWire))
	good.Blobs[0][31] ^= 1
	require.NoError(t, good.MarshalBinaryWrapped(&badWire))
	good.Blobs[0][31] ^= 1
	return pool, good, badWire.Bytes(), goodWire.Bytes(), key
}

func TestFetchRetriesHonestSidecarAfterQueuedSidecarFails(t *testing.T) {
	for _, wrongVersion := range []bool{false, true} {
		name := "corrupted blob"
		if wrongVersion {
			name = "wrong wrapper version"
		}
		t.Run(name, func(t *testing.T) {
			expectedReason := txpoolcfg.UnmatchedBlobTxExt
			if wrongVersion {
				expectedReason = txpoolcfg.UnequalBlobTxExt
			}
			ctx := t.Context()
			pool, good, badWire, goodWire, _ := newFetchBlobRetryFixture(t, 0)
			if wrongVersion {
				var err error
				badWire, err = good.ConvertToV1()
				require.NoError(t, err)
			}
			hash := good.Hash()
			badBody := EncodePooledTransactions66([][]byte{badWire}, 1, nil)
			goodBody := EncodePooledTransactions66([][]byte{goodWire}, 1, nil)
			require.NotEqual(t, badBody, goodBody)
			client, sent, _ := newFetchTestClient(t)
			var wg sync.WaitGroup
			fetch := NewFetch(ctx, nil, pool, nil, pool.poolDB, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
			parses := countFetchTestParses(fetch)
			parse := NewTxnParseContext(pool.chainID).ChainIDRequired()
			parse.ValidateRLP(ValidateSerializedTxn)
			for _, wire := range [][]byte{badWire, goodWire} {
				var slot TxnSlot
				_, err := parse.ParseTransaction(wire, 0, &slot, make([]byte, 20), false, true, nil)
				require.NoError(t, err)
				require.Equal(t, [32]byte(hash), slot.IDHash)
				require.NoError(t, fetch.checkBlobSidecar(&slot))
			}
			badReply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: badBody, PeerId: peerID}
			goodReply := &sentryproto.InboundMessage{
				Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: goodBody, PeerId: gointerfaces.ConvertHashToH512([64]byte{2}),
			}
			receiveFetchTestMessages(t, fetch, client,
				func() *sentryproto.InboundMessage { return badReply },
				func() *sentryproto.InboundMessage {
					require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
					return goodReply
				},
				func() *sentryproto.InboundMessage {
					require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
					reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
					require.NoError(t, err)
					require.Equal(t, []txpoolcfg.DiscardReason{expectedReason}, reasons)
					require.Empty(t, pool.unprocessedRemoteTxns.Txns)
					require.Empty(t, pool.byHash)
					return badReply
				},
				func() *sentryproto.InboundMessage {
					require.Empty(t, pool.unprocessedRemoteTxns.Txns, "the invalid sidecar must stay suppressed")
					return goodReply
				},
				func() *sentryproto.InboundMessage {
					require.Len(t, pool.unprocessedRemoteTxns.Txns, 1, "the honest sidecar must be retried after the queued sidecar is rejected")
					require.Equal(t, byte(1), pool.unprocessedRemoteTxns.Txns[0].BlobBundles[0].Blob[31])
					reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
					require.NoError(t, err)
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
					require.Contains(t, pool.byHash, string(hash[:]))
					return nil
				},
			)
			require.Equal(t, int64(3), parses())
			require.Empty(t, sent)
		})
	}
}

func TestFetchPreOsakaV1SidecarDoesNotPenalizePeer(t *testing.T) {
	ctx := t.Context()
	pool, good, _, _, _ := newFetchBlobRetryFixture(t, 0)
	require.False(t, pool.isOsaka())
	wire, err := good.ConvertToV1()
	require.NoError(t, err)

	synctest.Test(t, func(t *testing.T) {
		client, _, penalties := newFetchTestClient(t)
		var wg sync.WaitGroup
		fetch := NewFetch(ctx, nil, pool, nil, pool.poolDB, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
		receiveFetchTestMessages(t, fetch, client,
			func() *sentryproto.InboundMessage {
				return &sentryproto.InboundMessage{
					Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, PeerId: peerID,
					Data: EncodePooledTransactions66([][]byte{wire}, 1, nil),
				}
			},
			func() *sentryproto.InboundMessage {
				require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
				require.NoError(t, pool.processRemoteTxns(ctx))
				return nil
			},
		)
		synctest.Wait()
		require.Empty(t, penalties)
		require.Empty(t, pool.byHash)
	})
}

func TestFetchBlobCapacityRejections(t *testing.T) {
	for _, tc := range []struct {
		name         string
		badSidecar   bool
		accountLimit bool
		remaining    int
	}{
		{"blob limit with valid sidecar", false, false, 2},
		{"blob limit with invalid sidecar", true, false, 2},
		{"account limit", false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			pool, template, _, _, key := newFetchBlobRetryFixture(t, 0)
			if tc.accountLimit {
				pool.cfg.AccountSlots = 1
			} else {
				pool.cfg.BlobSlots = 2
			}
			makeSigned := func(nonce uint64) *types.BlobTxWrapper {
				wrapper := types.CopyTxs(types.Transactions{template})[0].(*types.BlobTxWrapper)
				wrapper.Tx.Nonce = nonce
				signed, err := types.SignTx(wrapper, *types.LatestSignerForChainID(&pool.chainID), key)
				require.NoError(t, err)
				return signed.(*types.BlobTxWrapper)
			}
			parse := NewTxnParseContext(pool.chainID).ChainIDRequired()
			parse.ValidateRLP(ValidateSerializedTxn)
			var hashes []common.Hash
			for nonce := range uint64(2) {
				signed := makeSigned(nonce)
				var encoded bytes.Buffer
				require.NoError(t, signed.MarshalBinaryWrapped(&encoded))
				var slot TxnSlot
				sender := make([]byte, 20)
				_, err := parse.ParseTransaction(encoded.Bytes(), 0, &slot, sender, false, true, nil)
				require.NoError(t, err)
				var local TxnSlots
				local.Append(&slot, sender, true)
				reasons, err := pool.AddLocalTxns(ctx, local)
				require.NoError(t, err)
				require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				hashes = append(hashes, signed.Hash())
			}
			require.Len(t, pool.byHash, 2)
			signed := makeSigned(2)
			hash := signed.Hash()
			if tc.badSidecar {
				signed.Blobs[0][31] ^= 1
			}
			var encoded bytes.Buffer
			require.NoError(t, signed.MarshalBinaryWrapped(&encoded))
			client, _, penalties := newFetchTestClient(t)
			var wg sync.WaitGroup
			fetch := NewFetch(ctx, nil, pool, nil, pool.poolDB, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
			receiveFetchTestMessages(t, fetch, client,
				func() *sentryproto.InboundMessage {
					return &sentryproto.InboundMessage{
						Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, PeerId: peerID,
						Data: EncodePooledTransactions66([][]byte{encoded.Bytes()}, 1, nil),
					}
				},
				func() *sentryproto.InboundMessage {
					require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
					reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
					require.NoError(t, err)
					require.Len(t, reasons, 1)
					require.NotEqual(t, txpoolcfg.Success, reasons[0])
					require.NotContains(t, pool.byHash, string(hash[:]))
					require.Len(t, pool.byHash, tc.remaining)
					require.Equal(t, uint64(tc.remaining), pool.totalBlobsInPool.Load())
					require.Contains(t, pool.byHash, string(hashes[0][:]))
					if tc.remaining == 2 {
						require.Contains(t, pool.byHash, string(hashes[1][:]))
					} else {
						require.Equal(t, txpoolcfg.Spammer, reasons[0])
						require.NotContains(t, pool.byHash, string(hashes[1][:]))
					}
					return nil
				},
			)
			require.Empty(t, penalties)
		})
	}
}

func TestFetchRetriesMixedHonestPacketAfterNonceProgressAndQueuedSidecarFailure(t *testing.T) {
	ctx := t.Context()
	pool, good, badWire, goodWire, key := newFetchBlobRetryFixture(t, 65)
	hash := good.Hash()
	sender := crypto.PubkeyToAddress(key.PublicKey)
	parse := NewTxnParseContext(pool.chainID).ChainIDRequired()
	parse.ValidateRLP(ValidateSerializedTxn)
	var known TxnSlot
	parsedSender := make([]byte, 20)
	_, err := parse.ParseTransaction(goodWire, 0, &known, parsedSender, false, true, nil)
	require.NoError(t, err)
	require.Equal(t, sender[:], parsedSender)
	var local TxnSlots
	local.Append(&known, parsedSender, true)
	reasons, err := pool.AddLocalTxns(ctx, local)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	require.Contains(t, pool.byHash, string(hash[:]))
	txn := newTestTxnSlot(66, 0, 300_000, 300_000, 100_000).Txn.(*types.DynamicFeeTransaction)
	txn.ChainID = pool.chainID
	other, err := types.SignTx(txn, *types.LatestSignerForChainID(&pool.chainID), key)
	require.NoError(t, err)
	otherHash := other.Hash()
	badBody := EncodePooledTransactions66([][]byte{badWire}, 1, nil)
	goodBody := EncodePooledTransactions66([][]byte{goodWire, encodeFetchTestTransactions(t, other)[0]}, 1, nil)
	client, sent, _ := newFetchTestClient(t)
	var wg sync.WaitGroup
	fetch := NewFetch(ctx, nil, pool, nil, pool.poolDB, pool.chainID, log.New(), WithP2PFetcherWg(&wg))
	parses := countFetchTestParses(fetch)
	badReply := &sentryproto.InboundMessage{Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: badBody, PeerId: peerID}
	goodReply := &sentryproto.InboundMessage{
		Id: sentryproto.MessageId_POOLED_TRANSACTIONS_66, Data: goodBody, PeerId: gointerfaces.ConvertHashToH512([64]byte{2}),
	}
	receiveFetchTestMessages(t, fetch, client,
		func() *sentryproto.InboundMessage { return badReply },
		func() *sentryproto.InboundMessage {
			require.Empty(t, pool.unprocessedRemoteTxns.Txns)
			updateP2PRetryNonce(t, pool, sender, 0, 2)
			require.NotContains(t, pool.byHash, string(hash[:]))
			record, found := pool.discardReasonsLRU.Peek(string(hash[:]))
			require.True(t, found)
			require.Equal(t, txpoolcfg.NonceTooDistant, record.reason)
			updateP2PRetryNonce(t, pool, sender, 65, 3)
			return badReply
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1)
			require.Equal(t, byte(0), pool.unprocessedRemoteTxns.Txns[0].BlobBundles[0].Blob[31])
			return goodReply
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 2)
			require.Equal(t, [32]byte(otherHash), pool.unprocessedRemoteTxns.Txns[1].IDHash)
			reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.UnmatchedBlobTxExt, txpoolcfg.Success}, reasons)
			require.NotContains(t, pool.byHash, string(hash[:]))
			require.Contains(t, pool.byHash, string(otherHash[:]))
			record, found := pool.discardReasonsLRU.Peek(string(hash[:]))
			require.True(t, found)
			require.Equal(t, txpoolcfg.NonceTooDistant, record.reason)
			return goodReply
		},
		func() *sentryproto.InboundMessage {
			require.Len(t, pool.unprocessedRemoteTxns.Txns, 1, "a mixed honest packet must retain the retry opportunity for its previously queued blob")
			require.Equal(t, [32]byte(hash), pool.unprocessedRemoteTxns.Txns[0].IDHash)
			require.Equal(t, byte(1), pool.unprocessedRemoteTxns.Txns[0].BlobBundles[0].Blob[31])
			reasons, err := pool.addNewTxns(ctx, *pool.unprocessedRemoteTxns, true)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
			require.Contains(t, pool.byHash, string(hash[:]))
			require.Contains(t, pool.byHash, string(otherHash[:]))
			return nil
		},
	)
	require.Equal(t, int64(6), parses())
	require.Empty(t, sent)
}
