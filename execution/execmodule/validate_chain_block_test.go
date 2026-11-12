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
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
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

func TestValidateChainReturnsMissingSegmentForUnreadableStoredHeader(t *testing.T) {
	for _, count := range []int{1, 17} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			m, _, _ := newMetricsTester(t)
			pack, err := m.GenerateChain(count, nil)
			require.NoError(t, err)
			header := pack.Blocks[count-1].Header()
			header.BaseFee = nil
			block := types.NewBlockFromNetwork(header, pack.Blocks[count-1].Body(), pack.Blocks[count-1].BlockAccessListSidecar())
			blocks := append([]*types.Block(nil), pack.Blocks...)
			blocks[count-1] = block
			status, err := m.InsertBlocks(t.Context(), blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)

			var result execmodule.ValidationResult
			var validationErr error
			require.NotPanics(t, func() {
				result, validationErr = m.ValidateChain(t.Context(), block)
			})
			require.NoError(t, validationErr)
			require.Equal(t, execmodule.ExecutionStatusMissingSegment, result.ValidationStatus)
		})
	}
}
