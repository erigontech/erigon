// Copyright 2024 The Erigon Authors
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
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestGetBlobsRemovedAfterMined(t *testing.T) {
	require := require.New(t)
	pool, ctx := newGetBlobsTestPool(t, 1)

	blobHashes := addTestBlobTxn(t, pool, ctx, 1, 0)

	before := pool.GetBlobs(blobHashes)
	require.Len(before, len(blobHashes))
	for i, bb := range before {
		require.NotNil(bb.Blob, "blob %d should be present before mining", i)
	}

	var addr [20]byte
	addr[0] = 1
	acc := accounts3.Account{Nonce: 1, Balance: *uint256.NewInt(1 * common.Ether), CodeHash: accounts3.EmptyCodeHash, Incarnation: 1}
	blockChange := &remoteproto.StateChangeBatch{
		StateVersionId:       1,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHeight: 1,
			BlockHash:   gointerfaces.ConvertHashToH256([32]byte{1}),
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    accounts3.SerialiseV3(&acc),
			}},
		}},
	}
	minedBlob := makeBlobTxn()
	minedBlob.IDHash[0] = 1
	minedBlob.Nonce = 0
	minedBlob.Txn.(*types.BlobTx).Nonce = 0
	var minedSlots TxnSlots
	minedSlots.Append(&minedBlob, addr[:], true)
	require.NoError(pool.OnNewBlock(ctx, blockChange, TxnSlots{}, TxnSlots{}, minedSlots))

	after := pool.GetBlobs(blobHashes)
	require.Len(after, len(blobHashes))
	for i, bb := range after {
		require.Nil(bb.Blob, "blob %d should be gone after mining", i)
	}
}

func TestGetBlobsUnknownHash(t *testing.T) {
	require := require.New(t)
	pool, ctx := newGetBlobsTestPool(t, 1)
	realHashes := addTestBlobTxn(t, pool, ctx, 1, 0)

	query := []common.Hash{{0xaa}, realHashes[0], {0xbb}, realHashes[1], {0xcc}}
	got := pool.GetBlobs(query)
	require.Len(got, len(query))
	require.Nil(got[0].Blob)
	require.NotNil(got[1].Blob)
	require.Nil(got[2].Blob)
	require.NotNil(got[3].Blob)
	require.Nil(got[4].Blob)
}

func newGetBlobsTestPool(tb testing.TB, numAccounts int) (*TxPool, context.Context) {
	tb.Helper()
	ch := make(chan Announcements, 5)
	coreDB := temporaltest.NewTestDB(tb, datadir.New(tb.TempDir()))
	db := memdb.NewTestPoolDB(tb)
	cfg := txpoolcfg.DefaultConfig
	cfg.TotalBlobPoolLimit = 1000
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, ch, db, coreDB, cfg, sendersCache, testforks.Forks["Cancun"], nil, nil, func() {}, nil, nil, log.New(), WithFeeCalculator(nil))
	require.NoError(tb, err)
	require.NotNil(tb, pool)
	pool.blockGasLimit.Store(30000000)

	h1 := gointerfaces.ConvertHashToH256([32]byte{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:       0,
		PendingBlockBaseFee:  200_000,
		BlockGasLimit:        math.MaxUint64,
		PendingBlobFeePerGas: 100_000,
		ChangeBatch:          []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h1}},
	}
	acc := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(1 * common.Ether), CodeHash: accounts3.EmptyCodeHash, Incarnation: 1}
	v := accounts3.SerialiseV3(&acc)
	var addr [20]byte
	for i := 0; i < numAccounts; i++ {
		addr[0] = uint8(i + 1)
		change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    v,
		})
	}
	require.NoError(tb, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	return pool, ctx
}

func addTestBlobTxn(tb testing.TB, pool *TxPool, ctx context.Context, senderByte byte, nonce uint64) []common.Hash {
	tb.Helper()
	blobTxn := makeBlobTxn()
	blobTxn.IDHash[0] = senderByte
	blobTxn.IDHash[1] = byte(nonce)
	blobTxn.Nonce = nonce
	bt := blobTxn.Txn.(*types.BlobTx)
	bt.Nonce = nonce
	bt.GasLimit = 50000
	var addr [20]byte
	addr[0] = senderByte
	var slots TxnSlots
	slots.Append(&blobTxn, addr[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(tb, err)
	for _, r := range reasons {
		require.Equal(tb, txpoolcfg.Success, r, r.String())
	}
	return blobTxn.GetBlobHashes()
}
