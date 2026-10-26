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

package p2p

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/types"
)

// headerWalkChain builds n linked headers numbered 1..n; headers[0] is the local connection point.
func headerWalkChain(n int) []*types.Header {
	headers := make([]*types.Header, n)
	parentHash := common.Hash{}
	withdrawalsHash := empty.RootHash
	for i := range headers {
		headers[i] = &types.Header{
			Number:          *uint256.NewInt(uint64(i + 1)),
			ParentHash:      parentHash,
			TxHash:          empty.RootHash,
			UncleHash:       empty.UncleHash,
			BaseFee:         uint256.NewInt(1),
			WithdrawalsHash: &withdrawalsHash,
		}
		parentHash = headers[i].Hash()
	}
	return headers
}

// lateJoinHeaderFetcher serves the chain from every peer, except that peer 1 stops answering header
// batches after its first one, and peer 2 connects only once the walk is under way.
type lateJoinHeaderFetcher struct {
	chainServingBbdFetcher
	tracker       *PeerTracker
	mu            sync.Mutex
	peer1Batches  atomic.Int64
	headerBatches map[PeerId]int
}

func (s *lateJoinHeaderFetcher) serveBatch(peerId *PeerId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headerBatches[*peerId]++
	if *peerId != *PeerIdFromUint64(1) {
		return nil
	}
	if s.peer1Batches.Add(1) > 1 {
		return context.DeadlineExceeded
	}
	if s.tracker != nil {
		s.tracker.PeerConnected(PeerIdFromUint64(2))
	}
	return nil
}

func (s *lateJoinHeaderFetcher) FetchHeadersBackwards(ctx context.Context, hash common.Hash, amount uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	if amount > 1 {
		if err := s.serveBatch(peerId); err != nil {
			return FetcherResponse[[]*types.Header]{}, err
		}
	}
	return s.chainServingBbdFetcher.FetchHeadersBackwards(ctx, hash, amount, peerId, opts...)
}

func (s *lateJoinHeaderFetcher) FetchHeaders(ctx context.Context, start, end uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	if err := s.serveBatch(peerId); err != nil {
		return FetcherResponse[[]*types.Header]{}, err
	}
	return s.chainServingBbdFetcher.FetchHeaders(ctx, start, end, peerId, opts...)
}

func (s *chainServingBbdFetcher) FetchHeaders(_ context.Context, start, end uint64, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	return FetcherResponse[[]*types.Header]{Data: s.headers[start-1 : end-1]}, nil
}

func TestBackwardBlockDownloader_HeaderWalkUsesPeersThatConnectAfterItStarts(t *testing.T) {
	headers := headerWalkChain(1501)
	fetcher := &lateJoinHeaderFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}, headerBatches: map[PeerId]int{}}
	bbd := newTestBbd(t, fetcher)
	fetcher.tracker = bbd.peerTracker
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]})
	require.NoError(t, err)
	var blocks int
	for {
		res, err := feed.Next(ctx)
		require.NoError(t, err)
		if res.Err != nil || res.Blocks == nil {
			require.NoError(t, res.Err)
			break
		}
		blocks += len(res.Blocks)
	}
	require.Equal(t, len(headers)-1, blocks)
	require.Positive(t, fetcher.headerBatches[*PeerIdFromUint64(2)])
}

func TestBackwardBlockDownloader_HeaderWalkFailsWhenRefreshedPeersMakeNoProgress(t *testing.T) {
	headers := headerWalkChain(1501)
	fetcher := &lateJoinHeaderFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}, headerBatches: map[PeerId]int{}}
	bbd := newTestBbd(t, fetcher)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]})
	require.NoError(t, err)
	_, err = feed.Next(ctx)
	require.ErrorContains(t, err, "all peers exhausted")
	require.EqualValues(t, 4, fetcher.peer1Batches.Load())
}

func TestBackwardBlockDownloader_PinnedHeaderWalkKeepsItsPeer(t *testing.T) {
	headers := headerWalkChain(1501)
	fetcher := &lateJoinHeaderFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}, headerBatches: map[PeerId]int{}}
	bbd := newTestBbd(t, fetcher)
	fetcher.tracker = bbd.peerTracker
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]}, WithPeerId(PeerIdFromUint64(1)))
	require.NoError(t, err)
	_, err = feed.Next(ctx)
	require.ErrorContains(t, err, "all peers exhausted")
	require.Zero(t, fetcher.headerBatches[*PeerIdFromUint64(2)])
}

// concurrencyHeaderFetcher serves the chain by hash and by number and records how many header batch
// requests overlap; byNumberFork, when set, is served by number instead of the chain for the ranges it covers.
type concurrencyHeaderFetcher struct {
	chainServingBbdFetcher
	byNumberFork map[uint64]*types.Header
	calls        atomic.Int64
	inFlight     atomic.Int64
	maxInFlight  atomic.Int64
}

func (s *concurrencyHeaderFetcher) track() func() {
	s.calls.Add(1)
	n := s.inFlight.Add(1)
	for {
		m := s.maxInFlight.Load()
		if n <= m || s.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	return func() { s.inFlight.Add(-1) }
}

func (s *concurrencyHeaderFetcher) FetchHeadersBackwards(ctx context.Context, hash common.Hash, amount uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	if amount > 1 {
		defer s.track()()
	}
	return s.chainServingBbdFetcher.FetchHeadersBackwards(ctx, hash, amount, peerId, opts...)
}

func (s *concurrencyHeaderFetcher) FetchHeaders(_ context.Context, start, end uint64, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	defer s.track()()
	out := make([]*types.Header, 0, end-start)
	for num := start; num < end; num++ {
		h := s.headers[num-1]
		if forked, ok := s.byNumberFork[num]; ok {
			h = forked
		}
		out = append(out, h)
	}
	return FetcherResponse[[]*types.Header]{Data: out}, nil
}

func drainHeaderWalkFeed(ctx context.Context, t *testing.T, feed BbdResultFeed) []*types.Block {
	var blocks []*types.Block
	for {
		res, err := feed.Next(ctx)
		require.NoError(t, err)
		if res.Err != nil || res.Blocks == nil {
			require.NoError(t, res.Err)
			return blocks
		}
		blocks = append(blocks, res.Blocks...)
	}
}

func TestBackwardBlockDownloader_HeaderWalkKeepsSeveralRequestsInFlight(t *testing.T) {
	headers := headerWalkChain(5001)
	fetcher := &concurrencyHeaderFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}}
	bbd := newTestBbd(t, fetcher)
	for i := uint64(2); i <= 4; i++ {
		bbd.peerTracker.PeerConnected(PeerIdFromUint64(i))
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]})
	require.NoError(t, err)
	blocks := drainHeaderWalkFeed(ctx, t, feed)
	require.Len(t, blocks, len(headers)-1)
	for i, b := range blocks {
		require.Equal(t, headers[i+1].Hash(), b.Hash())
	}
	require.GreaterOrEqual(t, fetcher.maxInFlight.Load(), int64(4))
}

func TestBackwardBlockDownloader_HeaderWalkRefetchesByHashWhenRangeDoesNotLink(t *testing.T) {
	headers := headerWalkChain(5001)
	forked := types.CopyHeader(headers[3999])
	forked.Extra = []byte("fork")
	fetcher := &concurrencyHeaderFetcher{
		chainServingBbdFetcher: chainServingBbdFetcher{headers: headers},
		byNumberFork:           map[uint64]*types.Header{forked.Number.Uint64(): forked},
	}
	bbd := newTestBbd(t, fetcher)
	bbd.peerTracker.PeerConnected(PeerIdFromUint64(2))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]})
	require.NoError(t, err)
	blocks := drainHeaderWalkFeed(ctx, t, feed)
	require.Len(t, blocks, len(headers)-1)
	for i, b := range blocks {
		require.Equal(t, headers[i+1].Hash(), b.Hash(), "block %d", b.NumberU64())
	}
}

// rangeFailingFetcher fails every by-number range, and the by-hash batch call numbered failHashCall when set.
type rangeFailingFetcher struct {
	chainServingBbdFetcher
	failHashCall int64
	hashCalls    atomic.Int64
	rangeCalls   atomic.Int64
}

func (s *rangeFailingFetcher) FetchHeaders(context.Context, uint64, uint64, *PeerId, ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	s.rangeCalls.Add(1)
	return FetcherResponse[[]*types.Header]{}, context.DeadlineExceeded
}

func (s *rangeFailingFetcher) FetchHeadersBackwards(ctx context.Context, hash common.Hash, amount uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	if amount > 1 && s.hashCalls.Add(1) == s.failHashCall {
		return FetcherResponse[[]*types.Header]{}, context.DeadlineExceeded
	}
	return s.chainServingBbdFetcher.FetchHeadersBackwards(ctx, hash, amount, peerId, opts...)
}

func TestBackwardBlockDownloader_HeaderWalkRangeFailuresDoNotUseUpPeers(t *testing.T) {
	headers := headerWalkChain(1501)
	fetcher := &rangeFailingFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}, failHashCall: 2}
	bbd := newTestBbd(t, fetcher)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]})
	require.NoError(t, err)
	require.Len(t, drainHeaderWalkFeed(ctx, t, feed), len(headers)-1)
}

func TestBackwardBlockDownloader_PinnedHeaderWalkFallsBackToHashAfterRangeFailure(t *testing.T) {
	headers := headerWalkChain(1501)
	fetcher := &rangeFailingFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}}
	bbd := newTestBbd(t, fetcher)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]}, WithPeerId(PeerIdFromUint64(1)))
	require.NoError(t, err)
	require.Len(t, drainHeaderWalkFeed(ctx, t, feed), len(headers)-1)
	require.EqualValues(t, 1, fetcher.rangeCalls.Load())
}

func TestBackwardBlockDownloader_HeaderWalkRequestsNothingPastChainLengthLimit(t *testing.T) {
	headers := headerWalkChain(6001)
	fetcher := &concurrencyHeaderFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}}
	bbd := newTestBbd(t, fetcher)
	bbd.peerTracker.PeerConnected(PeerIdFromUint64(2))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]}, WithChainLengthLimit(5000))
	require.NoError(t, err)
	_, err = feed.Next(ctx)
	require.ErrorIs(t, err, ErrChainLengthExceedsLimit)
	require.EqualValues(t, 10, fetcher.calls.Load())
}
