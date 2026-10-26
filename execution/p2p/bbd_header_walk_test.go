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
	peer1Batches  atomic.Int64
	headerBatches map[PeerId]int
}

func (s *lateJoinHeaderFetcher) FetchHeadersBackwards(ctx context.Context, hash common.Hash, amount uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	if amount > 1 {
		s.headerBatches[*peerId]++
		if *peerId == *PeerIdFromUint64(1) {
			if s.peer1Batches.Add(1) > 1 {
				return FetcherResponse[[]*types.Header]{}, context.DeadlineExceeded
			}
			if s.tracker != nil {
				s.tracker.PeerConnected(PeerIdFromUint64(2))
			}
		}
	}
	return s.chainServingBbdFetcher.FetchHeadersBackwards(ctx, hash, amount, peerId, opts...)
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
	require.EqualValues(t, 3, fetcher.peer1Batches.Load())
}
