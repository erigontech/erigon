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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/exec"
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

func TestValidateChainWritesTheSendersOfARunningRecovery(t *testing.T) {
	m, privKey, senderAddr := newMetricsTester(t)
	pack, err := m.GenerateChain(1, sendTo(t, m, privKey, common.Address{0xaa}, 1))
	require.NoError(t, err)
	enc, err := rlp.EncodeToBytes(pack.Blocks[0])
	require.NoError(t, err)
	block := new(types.Block)
	require.NoError(t, rlp.DecodeBytes(enc, block))

	status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	signer := types.MakeSigner(m.ChainConfig, block.NumberU64(), block.Time())
	m.ExecModule.AddSendersRecovery(block.Hash(), exec.StartSendersRecovery(t.Context(), signer, block.Transactions()))
	result, err := m.ValidateChain(t.Context(), block)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	_, err = m.UpdateForkChoice(t.Context(), block.Header())
	require.NoError(t, err)

	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		senders, err := rawdb.ReadSenders(tx, block.Hash(), block.NumberU64())
		require.NoError(t, err)
		require.Equal(t, []common.Address{senderAddr}, senders)
		return nil
	}))
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

func TestInvalidPayloadPurgeVsBodyGetters(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	m.ExecModule.SetPublishedSD(m.Notifications.Events.LatestSD)
	pack, err := m.GenerateChain(8, sendTo(t, m, key, common.Address{0x0a}, 1))
	require.NoError(t, err)
	hashes := make([]common.Hash, 0, len(pack.Blocks))
	for _, block := range pack.Blocks {
		hashes = append(hashes, block.Hash())
	}

	ctx := t.Context()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := g; !stop.Load(); i++ {
				var err error
				if i%2 == 0 {
					_, _, err = m.ExecModule.FrozenBlocks(ctx)
				} else {
					_, err = m.ExecModule.GetBodiesByHashes(ctx, hashes)
				}
				if err != nil {
					t.Errorf("getter: %v", err)
					return
				}
			}
		})
	}
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()

	parent := m.Genesis.Hash()
	for _, block := range pack.Blocks {
		bad := tamperBlockGasUsed(t, block, 21_000)
		_, err := m.InsertBlocks(ctx, []*types.Block{bad})
		require.NoError(t, err)
		result, err := m.ValidateChain(ctx, bad)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
		require.Equal(t, parent, result.LatestValidHash)

		_, err = m.InsertBlocks(ctx, []*types.Block{block})
		require.NoError(t, err)
		valid, err := m.ValidateChain(ctx, block)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, valid.ValidationStatus)
		fcu, err := m.UpdateForkChoice(ctx, block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
		m.ExecModule.WaitIdle(ctx)
		parent = block.Hash()
	}
}
