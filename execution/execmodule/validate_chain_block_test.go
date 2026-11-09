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

package execmodule_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

type bodyReadCounter struct {
	dbservices.FullBlockReader
	hash  atomic.Pointer[common.Hash]
	reads atomic.Int32
}

func (r *bodyReadCounter) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Body, error) {
	if h := r.hash.Load(); h != nil && *h == hash {
		r.reads.Add(1)
	}
	return r.FullBlockReader.BodyWithTransactions(ctx, tx, hash, blockNum)
}

func TestValidateChainExecutesTheGivenBlock(t *testing.T) {
	counter := &bodyReadCounter{}
	m, privKey, _ := newMetricsTester(t, execmoduletester.WithBlockReaderWrap(func(br dbservices.FullBlockReader) dbservices.FullBlockReader {
		counter.FullBlockReader = br
		return counter
	}))
	pack, err := m.GenerateChain(1, sendTo(t, m, privKey, common.Address{0xaa}, 1))
	require.NoError(t, err)
	enc, err := rlp.EncodeToBytes(pack.Blocks[0])
	require.NoError(t, err)
	block := new(types.Block)
	require.NoError(t, rlp.DecodeBytes(enc, block))
	txn := block.Transactions()[0]
	_, ok := txn.GetSender()
	require.False(t, ok)

	status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	hash := block.Hash()
	counter.hash.Store(&hash)
	result, err := m.ValidateChain(t.Context(), block)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)

	require.Zero(t, counter.reads.Load(), "validation read back the body of the block it was given")
	_, ok = txn.GetSender()
	require.True(t, ok, "validation executed a copy of the block instead of the transactions it was given")
	body, ok := m.ReadAheader.ReadBodyWithTransactions(hash)
	require.True(t, ok)
	require.Same(t, &block.Transactions()[0], &body.Transactions[0], "validation copied the bare transaction slice")
}

func TestValidateChainPreservesBlobSidecar(t *testing.T) {
	m, key, _ := newMetricsTester(t, execmoduletester.WithTxPool())
	pack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	parent := pack.TopBlock
	require.NoError(t, m.WaitForTxPool(t.Context(), parent.Hash()))
	wrapped := types.MakeV1WrappedBlobTxn(m.ChainConfig.ChainID)
	signed, err := types.SignTx(&wrapped.Tx, *types.LatestSignerForChainID(m.ChainConfig.ChainID), key)
	require.NoError(t, err)
	v, r, s := signed.RawSignatureValues()
	wrapped.Tx.V.Set(v)
	wrapped.Tx.R.Set(r)
	wrapped.Tx.S.Set(s)
	var before bytes.Buffer
	require.NoError(t, wrapped.MarshalBinaryWrapped(&before))
	added, err := m.TxPoolGrpcServer.Add(t.Context(), &txpoolproto.AddRequest{RlpTxs: [][]byte{before.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, []string{"success"}, added.Errors)
	payloadID, err := m.AssembleBlock(t.Context(), &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		Withdrawals:           types.Withdrawals{},
		ParentBeaconBlockRoot: &common.Hash{},
		SlotNumber:            syntheticSlotNumber(parent),
	})
	require.NoError(t, err)
	block, err := m.GetAssembledBlock(t.Context(), payloadID)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 1)
	txn := block.Transactions()[0]
	require.IsType(t, wrapped, txn)
	status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	result, err := m.ValidateChain(t.Context(), block)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	require.Same(t, txn, block.Transactions()[0])
	var after bytes.Buffer
	require.NoError(t, block.Transactions()[0].(*types.BlobTxWrapper).MarshalBinaryWrapped(&after))
	require.Equal(t, before.Bytes(), after.Bytes())
}
