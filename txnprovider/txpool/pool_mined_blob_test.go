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
	"context"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestDeleteMinedBlobTxnUpdatesMovedIndex(t *testing.T) {
	pool := &TxPool{
		minedBlobTxnsByBlock: make(map[uint64][]*metaTxn),
		minedBlobTxnsByHash:  make(map[string]*metaTxn),
	}
	hashes := []string{"A", "B", "C"}
	items := make([]*metaTxn, len(hashes))
	for i, hash := range hashes {
		items[i] = &metaTxn{
			minedBlockNum: 1,
			bestIndex:     i,
		}
		pool.minedBlobTxnsByHash[hash] = items[i]
	}
	pool.minedBlobTxnsByBlock[1] = items

	pool.deleteMinedBlobTxn(hashes[0])
	pool.deleteMinedBlobTxn(hashes[2])
	pool.deleteMinedBlobTxn(hashes[1])
	if len(pool.minedBlobTxnsByBlock[1]) != 0 {
		t.Fatal("expected all mined blob transactions to be deleted")
	}
}

func TestFinalizeMinedBlobsAfterDeletion(t *testing.T) {
	pool := &TxPool{
		minedBlobTxnsByBlock: make(map[uint64][]*metaTxn),
		minedBlobTxnsByHash:  make(map[string]*metaTxn),
	}
	txns := make([]*TxnSlot, 4)
	for i := range txns {
		txns[i] = newTestBlobTxnSlot(uint64(i), 1, 1, 1, 21000)
		txns[i].IDHash[0] = byte(i + 1)
		require.NoError(t, pool.processMinedFinalizedBlobs(txns[i:i+1], uint64(i+1), 0))
	}

	pool.deleteMinedBlobTxn(string(txns[1].IDHash[:]))
	pool.deleteMinedBlobTxn(string(txns[2].IDHash[:]))
	require.NoError(t, pool.processMinedFinalizedBlobs(nil, 5, 3))

	for i := range 3 {
		require.NotContains(t, pool.minedBlobTxnsByBlock, uint64(i+1))
		require.NotContains(t, pool.minedBlobTxnsByHash, string(txns[i].IDHash[:]))
	}
	require.Len(t, pool.minedBlobTxnsByBlock, 1)
	require.Len(t, pool.minedBlobTxnsByHash, 1)
	require.Same(t, txns[3], pool.minedBlobTxnsByBlock[4][0].TxnSlot)
	require.Same(t, pool.minedBlobTxnsByBlock[4][0], pool.minedBlobTxnsByHash[string(txns[3].IDHash[:])])
}

// Mined txns come from the state-change stream without blobs, so only the pooled
// copy can bring the blobs back after an unwind.
type minedBlobReorgTest struct {
	ctx     context.Context
	pool    *TxPool
	sender  common.Address
	blobTxn TxnSlot
	hash    string
}

// newMinedBlobReorgTest starts a pool at block 0 holding one local blob txn with nonce 2.
func newMinedBlobReorgTest(t *testing.T) *minedBlobReorgTest {
	r := newMinedBlobReorgPool(t)
	var local TxnSlots
	local.Append(&r.blobTxn, r.sender[:], true)
	reasons, err := r.pool.AddLocalTxns(r.ctx, local)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	return r
}

// newMinedBlobReorgPool starts a pool at block 0 and builds a blob txn with nonce 2 without adding it.
func newMinedBlobReorgPool(t *testing.T) *minedBlobReorgTest {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	chainConfig := *testforks.Forks["Cancun"]
	chainConfig.ChainID = uint256.NewInt(5)
	pool, err := New(ctx, make(chan Announcements, 5), mdbxtest.NewTestPoolDB(t), coreDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), &chainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)
	r := &minedBlobReorgTest{ctx: ctx, pool: pool, sender: common.Address{1}}
	require.NoError(t, pool.OnNewBlock(ctx, r.block(0, 2, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, TxnSlots{}))

	// Parsed, so Rlp and blobs share one buffer as in production.
	wrapped := types.MakeWrappedBlobTxn(chainConfig.ChainID)
	wrapped.Tx.Nonce = 2
	wrapped.Tx.MaxFeePerBlobGas = *uint256.NewInt(200_000)
	var encoded bytes.Buffer
	require.NoError(t, wrapped.MarshalBinaryWrapped(&encoded))
	parseCtx := NewTxnParseContext(*chainConfig.ChainID)
	parseCtx.WithSender(false)
	_, err = parseCtx.ParseTransaction(encoded.Bytes(), 0, &r.blobTxn, nil, false, true, nil)
	require.NoError(t, err)
	r.hash = string(r.blobTxn.IDHash[:])
	return r
}

func (r *minedBlobReorgTest) block(height byte, nonce uint64, direction remoteproto.Direction) *remoteproto.StateChangeBatch {
	account := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash, Incarnation: 1}
	return &remoteproto.StateChangeBatch{
		PendingBlockBaseFee:  200_000,
		PendingBlobFeePerGas: 100_000,
		BlockGasLimit:        math.MaxUint64,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: uint64(height),
			BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{height}),
			Direction:   direction,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(r.sender),
				Data:    accounts.SerialiseV3(&account),
			}},
		}},
	}
}

// streamed is the txn as the state-change stream delivers it: without blobs.
func (r *minedBlobReorgTest) streamed() TxnSlots {
	txn := r.blobTxn
	txn.BlobBundles = nil
	var txns TxnSlots
	txns.Append(&txn, r.sender[:], false)
	return txns
}

func (r *minedBlobReorgTest) mine(t *testing.T, height byte, nonce uint64) {
	require.NoError(t, r.pool.OnNewBlock(r.ctx, r.block(height, nonce, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, r.streamed()))
}

// reorgOneBlock unwinds block 1 back to block 0 and applies block 1', which mines the txn again if mined is set.
func (r *minedBlobReorgTest) reorgOneBlock(t *testing.T, newHash byte, mined bool) {
	reorg := r.block(0, 2, remoteproto.Direction_UNWIND)
	minedTxns := TxnSlots{}
	if mined {
		minedTxns = r.streamed()
		reorg = r.block(0, 3, remoteproto.Direction_UNWIND)
	}
	reorg.ChangeBatch = append(reorg.ChangeBatch, &remoteproto.StateChange{BlockHeight: 1, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{newHash})})
	require.NoError(t, r.pool.OnNewBlock(r.ctx, reorg, TxnSlots{}, r.streamed(), minedTxns))
}

func TestMinedBlobTxnReturnsWithBlobsAfterUnwind(t *testing.T) {
	r := newMinedBlobReorgTest(t)
	r.mine(t, 1, 3)
	require.NotContains(t, r.pool.byHash, r.hash)
	require.Equal(t, uint64(1), r.pool.minedBlobTxnsByHash[r.hash].minedBlockNum)

	r.reorgOneBlock(t, 0x11, false)
	require.Contains(t, r.pool.byHash, r.hash)
	require.Len(t, r.pool.byHash[r.hash].TxnSlot.BlobBundles, 2)
}

// A remote blob txn mined before its queue is processed must be cached with the blobs of the queued copy.
func TestMinedQueuedRemoteBlobTxnReturnsWithBlobsAfterUnwind(t *testing.T) {
	r := newMinedBlobReorgPool(t)
	var remote TxnSlots
	remote.Append(&r.blobTxn, r.sender[:], false)
	r.pool.started.Store(true)
	r.pool.AddRemoteTxns(r.ctx, remote, nil, nil)
	r.mine(t, 1, 3)
	require.NoError(t, r.pool.processRemoteTxns(r.ctx))

	r.reorgOneBlock(t, 0x11, false)
	require.Contains(t, r.pool.byHash, r.hash)
	require.Len(t, r.pool.byHash[r.hash].TxnSlot.BlobBundles, 2)
}

func TestMinedBlobTxnKeepsRlpAfterFlushAndUnwind(t *testing.T) {
	r := newMinedBlobReorgTest(t)
	_, err := r.pool.flush(r.ctx)
	require.NoError(t, err)
	r.mine(t, 1, 3)
	_, err = r.pool.flush(r.ctx)
	require.NoError(t, err)

	r.reorgOneBlock(t, 0x11, false)
	require.NoError(t, r.pool.poolDB.View(r.ctx, func(tx kv.Tx) error {
		rlp, err := r.pool.GetRlp(tx, []byte(r.hash))
		require.NotEmpty(t, rlp)
		return err
	}))
}

// A blob txn loaded from the pool DB keeps its RLP, so it returns whole after it is mined, flushed out of the DB and unwound.
func TestMinedBlobTxnKeepsRlpAfterReloadFlushAndUnwind(t *testing.T) {
	r := newMinedBlobReorgTest(t)
	want := bytes.Clone(r.blobTxn.Rlp)
	_, err := r.pool.flush(r.ctx)
	require.NoError(t, err)

	reloaded, err := New(r.ctx, make(chan Announcements, 5), r.pool.poolDB, r.pool._chainDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), r.pool.chainConfig, nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)
	r.pool = reloaded
	require.NoError(t, r.pool.OnNewBlock(r.ctx, r.block(0, 2, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.NoError(t, r.pool.poolDB.View(r.ctx, func(tx kv.Tx) error { return r.pool.fromDB(r.ctx, tx) }))
	require.Contains(t, r.pool.byHash, r.hash)

	r.mine(t, 1, 3)
	_, err = r.pool.flush(r.ctx)
	require.NoError(t, err)
	r.reorgOneBlock(t, 0x11, false)
	require.NoError(t, r.pool.poolDB.View(r.ctx, func(tx kv.Tx) error {
		rlp, err := r.pool.GetRlp(tx, []byte(r.hash))
		require.Equal(t, want, rlp)
		return err
	}))
}

func TestMinedBlobTxnSurvivesReorgThatMinesItAgain(t *testing.T) {
	r := newMinedBlobReorgTest(t)
	r.mine(t, 1, 3)
	r.reorgOneBlock(t, 0x11, true)
	require.Len(t, r.pool.minedBlobTxnsByBlock[1], 1)
	require.Len(t, r.pool.minedBlobTxnsByHash[r.hash].TxnSlot.BlobBundles, 2)

	r.reorgOneBlock(t, 0x12, false)
	require.Contains(t, r.pool.byHash, r.hash)
	require.Len(t, r.pool.byHash[r.hash].TxnSlot.BlobBundles, 2)
}

// The new branch's head is past the depth bound of the old cache entry, which must still give the re-mined txn its blobs.
func TestMinedBlobTxnSurvivesDeepReorgThatMinesItAgain(t *testing.T) {
	r := newMinedBlobReorgTest(t)
	r.mine(t, 1, 3)
	for height := 2; height <= int(dbg.MaxReorgDepth); height++ {
		require.NoError(t, r.pool.OnNewBlock(r.ctx, r.block(byte(height), 3, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	}
	head := byte(2 + int(dbg.MaxReorgDepth))
	reorg := r.block(0, 3, remoteproto.Direction_UNWIND)
	reorg.ChangeBatch = append(reorg.ChangeBatch, &remoteproto.StateChange{BlockHeight: uint64(head), BlockHash: gointerfaces.ConvertHashToH256(common.Hash{head})})
	require.NoError(t, r.pool.OnNewBlock(r.ctx, reorg, TxnSlots{}, r.streamed(), r.streamed()))
	require.Len(t, r.pool.minedBlobTxnsByHash[r.hash].TxnSlot.BlobBundles, 2)
}

func TestMinedBlobCacheIsBoundedWithoutFinality(t *testing.T) {
	r := newMinedBlobReorgTest(t)
	r.mine(t, 1, 3)
	require.Contains(t, r.pool.minedBlobTxnsByHash, r.hash)
	for height := 2; height <= 2+int(dbg.MaxReorgDepth); height++ {
		require.NoError(t, r.pool.OnNewBlock(r.ctx, r.block(byte(height), 3, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	}
	require.NotContains(t, r.pool.minedBlobTxnsByHash, r.hash)
	require.NotContains(t, r.pool.minedBlobTxnsByBlock, uint64(1))
}

func TestUnwoundBlobTxnWithoutSidecarIsNotAdded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	pool, err := New(ctx, make(chan Announcements, 5), mdbxtest.NewTestPoolDB(t), coreDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)

	sender := common.Address{1}
	block := func(height byte, nonce uint64, direction remoteproto.Direction) *remoteproto.StateChangeBatch {
		account := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash, Incarnation: 1}
		return &remoteproto.StateChangeBatch{
			PendingBlockBaseFee:  200_000,
			PendingBlobFeePerGas: 100_000,
			BlockGasLimit:        math.MaxUint64,
			ChangeBatch: []*remoteproto.StateChange{{
				BlockHeight: uint64(height),
				BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{height}),
				Direction:   direction,
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(sender),
					Data:    accounts.SerialiseV3(&account),
				}},
			}},
		}
	}
	require.NoError(t, pool.OnNewBlock(ctx, block(0, 2, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, TxnSlots{}))

	blobTxn := makeBlobTxn()
	blobTxn.Nonce = 2
	blobTxn.BlobBundles = nil
	var mined TxnSlots
	mined.Append(&blobTxn, sender[:], false)
	require.NoError(t, pool.OnNewBlock(ctx, block(1, 3, remoteproto.Direction_FORWARD), TxnSlots{}, TxnSlots{}, mined))

	unwoundTxn := blobTxn
	var unwound TxnSlots
	unwound.Append(&unwoundTxn, sender[:], false)
	reorg := block(0, 2, remoteproto.Direction_UNWIND)
	reorg.ChangeBatch = append(reorg.ChangeBatch, &remoteproto.StateChange{BlockHeight: 1, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{0x11})})
	require.NoError(t, pool.OnNewBlock(ctx, reorg, TxnSlots{}, unwound, TxnSlots{}))
	require.NotContains(t, pool.byHash, string(blobTxn.IDHash[:]))
}

func TestSecondSameHeightReorgReinjectsMinedBlobTxn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	pool, err := New(ctx, make(chan Announcements, 5), mdbxtest.NewTestPoolDB(t), coreDB, txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig), testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(t, err)

	sender := common.Address{1}
	change := func(height, fork byte, nonce uint64, direction remoteproto.Direction) *remoteproto.StateChange {
		account := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash, Incarnation: 1}
		return &remoteproto.StateChange{
			BlockHeight: uint64(height),
			BlockHash:   gointerfaces.ConvertHashToH256(common.Hash{height, fork}),
			Direction:   direction,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(sender),
				Data:    accounts.SerialiseV3(&account),
			}},
		}
	}
	batch := func(changes ...*remoteproto.StateChange) *remoteproto.StateChangeBatch {
		return &remoteproto.StateChangeBatch{PendingBlockBaseFee: 200_000, PendingBlobFeePerGas: 100_000, BlockGasLimit: math.MaxUint64, ChangeBatch: changes}
	}
	reorgTo := func(fork byte, nonce uint64) *remoteproto.StateChangeBatch {
		return batch(change(0, 0, 2, remoteproto.Direction_UNWIND), change(1, fork, nonce, remoteproto.Direction_FORWARD))
	}
	require.NoError(t, pool.OnNewBlock(ctx, batch(change(0, 0, 2, remoteproto.Direction_FORWARD)), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	require.NoError(t, pool.OnNewBlock(ctx, batch(change(1, 1, 2, remoteproto.Direction_FORWARD)), TxnSlots{}, TxnSlots{}, TxnSlots{}))

	blobTxn := makeBlobTxn()
	blobTxn.Nonce = 2
	var mined TxnSlots
	mined.Append(&blobTxn, sender[:], false)
	require.NoError(t, pool.OnNewBlock(ctx, reorgTo(2, 3), TxnSlots{}, TxnSlots{}, mined))
	require.Contains(t, pool.minedBlobTxnsByHash, string(blobTxn.IDHash[:]))

	unwoundTxn := blobTxn
	unwoundTxn.BlobBundles = nil
	var unwound TxnSlots
	unwound.Append(&unwoundTxn, sender[:], false)
	require.NoError(t, pool.OnNewBlock(ctx, reorgTo(3, 2), TxnSlots{}, unwound, TxnSlots{}))

	require.Contains(t, pool.byHash, string(blobTxn.IDHash[:]), "the unwound txn is back in the pool")
	require.NotContains(t, pool.minedBlobTxnsByHash, string(blobTxn.IDHash[:]))
}

func TestMinedBlobTxnMinedAgainSurvivesFinalizingItsOldBlock(t *testing.T) {
	pool := &TxPool{
		minedBlobTxnsByBlock: make(map[uint64][]*metaTxn),
		minedBlobTxnsByHash:  make(map[string]*metaTxn),
	}
	blobTxn := newTestBlobTxnSlot(0, 1, 1, 1, 21000)
	blobTxn.IDHash[0] = 1
	hash := string(blobTxn.IDHash[:])
	mineAt := func(block uint64) {
		require.NoError(t, pool.processMinedFinalizedBlobs([]*TxnSlot{blobTxn}, block, 0))
	}
	mineAt(5)
	mineAt(5) // a same-height reorg to a block with the same txn
	mineAt(7)

	require.NoError(t, pool.processMinedFinalizedBlobs(nil, 7, 5))

	require.Contains(t, pool.minedBlobTxnsByHash, hash, "block 7 is not finalized")
	require.Equal(t, uint64(7), pool.minedBlobTxnsByHash[hash].minedBlockNum)
	require.Len(t, pool.minedBlobTxnsByBlock[7], 1)
}
