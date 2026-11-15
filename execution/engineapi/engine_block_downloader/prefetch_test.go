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

package engine_block_downloader

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

type countingFeed struct {
	batches []p2p.BlockBatchResult
	pulled  atomic.Int32
	err     error
}

func (f *countingFeed) Next(ctx context.Context) (p2p.BlockBatchResult, error) {
	i := int(f.pulled.Add(1)) - 1
	if i < len(f.batches) {
		return f.batches[i], nil
	}
	if f.err != nil {
		return p2p.BlockBatchResult{}, f.err
	}
	return p2p.BlockBatchResult{}, nil
}

func testBatches(n int) []p2p.BlockBatchResult {
	out := make([]p2p.BlockBatchResult, n)
	for i := range out {
		out[i] = p2p.BlockBatchResult{Blocks: []*types.Block{
			types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(uint64(i + 1))}, nil),
		}}
	}
	return out
}

func TestBatchPrefetcher_DownloadsWhileConsumerExecutes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	feed := &countingFeed{batches: testBatches(10)}
	p := newBatchPrefetcher(ctx, feed, 1<<30)

	first, err := p.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.Blocks[0].NumberU64())

	require.Eventually(t, func() bool { return feed.pulled.Load() == 11 }, 5*time.Second, time.Millisecond)

	for i := 2; i <= 10; i++ {
		batch, err := p.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(i), batch.Blocks[0].NumberU64())
	}
	batch, err := p.Next(ctx)
	require.NoError(t, err)
	require.Empty(t, batch.Blocks)
}

func TestBatchPrefetcher_StopsAtBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	batches := testBatches(10)
	size := int64(batches[0].Blocks[0].Size())
	feed := &countingFeed{batches: batches}
	p := newBatchPrefetcher(ctx, feed, 3*size)

	require.Eventually(t, func() bool { return feed.pulled.Load() == 4 }, 5*time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(4), feed.pulled.Load())

	_, err := p.Next(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return feed.pulled.Load() == 5 }, 5*time.Second, time.Millisecond)
}

func TestBatchSize_CountsBlockAccessList(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(1)}
	plain := batchSize([]*types.Block{types.NewBlockWithHeader(header, nil)})
	bal := types.BlockAccessList{types.AccountChanges{Address: common.HexToAddress("0x01")}}
	sidecar := types.NewBlockAccessListSidecar(bal)
	raw, err := sidecar.Bytes()
	require.NoError(t, err)
	withBAL := batchSize([]*types.Block{types.NewBlockWithHeader(header, sidecar)})
	require.Equal(t, plain+int64(len(raw)), withBAL)
}

func TestBatchPrefetcher_BatchLargerThanBudgetStillDelivered(t *testing.T) {
	feed := &countingFeed{batches: testBatches(2)}
	p := newBatchPrefetcher(t.Context(), feed, 1)
	for i := 1; i <= 2; i++ {
		batch, err := p.Next(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(i), batch.Blocks[0].NumberU64())
	}
}

func TestBatchPrefetcher_PassesFeedError(t *testing.T) {
	errFeed := errors.New("all peers exhausted")
	feed := &countingFeed{batches: testBatches(1), err: errFeed}
	p := newBatchPrefetcher(t.Context(), feed, 1<<30)
	_, err := p.Next(t.Context())
	require.NoError(t, err)
	_, err = p.Next(t.Context())
	require.ErrorIs(t, err, errFeed)
}

func TestBatchPrefetcher_CancelStopsProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	feed := &countingFeed{batches: testBatches(10)}
	p := newBatchPrefetcher(ctx, feed, 1)
	require.Eventually(t, func() bool { return feed.pulled.Load() >= 2 }, 5*time.Second, time.Millisecond)
	cancel()
	_, err := p.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Eventually(t, func() bool {
		_, open := <-p.ch
		return !open
	}, 5*time.Second, time.Millisecond)
}
