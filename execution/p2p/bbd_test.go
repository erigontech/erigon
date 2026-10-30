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
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/types"
)

// stubBbdFetcher serves the initial-header backward fetch and counts calls so tests can assert fail-fast behaviour.
type stubBbdFetcher struct {
	initialHeader         *types.Header
	headersBackwardsCalls atomic.Int64
}

func (s *stubBbdFetcher) FetchHeaders(context.Context, uint64, uint64, *PeerId, ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	return FetcherResponse[[]*types.Header]{}, errors.New("not implemented")
}

func (s *stubBbdFetcher) FetchHeadersBackwards(_ context.Context, hash common.Hash, amount uint64, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	s.headersBackwardsCalls.Add(1)
	if amount == 1 && hash == s.initialHeader.Hash() {
		return FetcherResponse[[]*types.Header]{Data: []*types.Header{s.initialHeader}}, nil
	}
	return FetcherResponse[[]*types.Header]{}, errors.New("unexpected backward header fetch")
}

func (s *stubBbdFetcher) FetchBodies(context.Context, []*types.Header, *PeerId, ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	return FetcherResponse[[]*types.Body]{}, errors.New("not implemented")
}

func (s *stubBbdFetcher) FetchBlocksBackwardsByHash(context.Context, common.Hash, uint64, *PeerId, ...FetcherOption) (FetcherResponse[[]*types.Block], error) {
	return FetcherResponse[[]*types.Block]{}, errors.New("not implemented")
}

type stubBbdHeaderReader struct{}

func (stubBbdHeaderReader) HeaderByHash(context.Context, common.Hash) (*types.Header, error) {
	return nil, nil
}

func newTestBbd(t *testing.T, fetcher Fetcher) *BackwardBlockDownloader {
	t.Helper()
	logger := testlog.Logger(t, log.LvlCrit)
	peerTracker := NewPeerTracker(logger, nil)
	peerTracker.PeerConnected(PeerIdFromUint64(1))
	return NewBackwardBlockDownloader(logger, fetcher, peerTracker, t.TempDir())
}

// EL behind the requested header by more than the limit: fail-fast at initial-header step.
func TestBackwardBlockDownloader_GapBehindCurrentHead_FailsFast(t *testing.T) {
	initialHeader := &types.Header{Number: *uint256.NewInt(500)}
	fetcher := &stubBbdFetcher{initialHeader: initialHeader}
	bbd := newTestBbd(t, fetcher)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(
		ctx, initialHeader.Hash(), stubBbdHeaderReader{},
		WithChainLengthLimit(96),
		WithChainLengthCurrentHead(100),
	)
	require.NoError(t, err)

	_, err = feed.Next(ctx)
	require.ErrorIs(t, err, ErrChainLengthExceedsLimit)
	require.EqualValues(t, 1, fetcher.headersBackwardsCalls.Load(),
		"should fail-fast at the initial-header check without fetching a header batch")
}

type bodyServingBbdFetcher struct {
	stubBbdFetcher
}

func (s *bodyServingBbdFetcher) FetchBodies(_ context.Context, headers []*types.Header, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	bodies := make([]*types.Body, len(headers))
	for i := range bodies {
		bodies[i] = &types.Body{Withdrawals: types.Withdrawals{}}
	}
	return FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

type fixedHeaderReader struct {
	header *types.Header
}

func (r fixedHeaderReader) HeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error) {
	if hash == r.header.Hash() {
		return r.header, nil
	}
	return nil, nil
}

type countingBALFetcher struct {
	calls atomic.Int64
}

func (s *countingBALFetcher) Fetch(_ context.Context, reqs []BALRequest, _ *PeerId, _ []PeerId, _ time.Duration, _ time.Duration) map[common.Hash]*types.BlockAccessListSidecar {
	s.calls.Add(1)
	return nil
}

type retryingBbdFetcher struct {
	bodyServingBbdFetcher
	balStarted <-chan struct{}
	peers      []PeerId
}

func (s *retryingBbdFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	s.peers = append(s.peers, *peerId)
	if len(s.peers) == 1 {
		<-s.balStarted
		return FetcherResponse[[]*types.Body]{}, NewErrMissingBodies(headers)
	}
	return s.bodyServingBbdFetcher.FetchBodies(ctx, headers, peerId, opts...)
}

type balFetcherFunc func(context.Context, []BALRequest) map[common.Hash]*types.BlockAccessListSidecar

func (f balFetcherFunc) Fetch(ctx context.Context, reqs []BALRequest, _ *PeerId, _ []PeerId, _ time.Duration, _ time.Duration) map[common.Hash]*types.BlockAccessListSidecar {
	return f(ctx, reqs)
}

func TestBackwardBlockDownloader_CancelsBALFetchOnBodyFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started := make(chan struct{})
		var balCalls int
		bal := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: common.Address{1}}})
		balHash, err := bal.Hash()
		require.NoError(t, err)
		balFetcher := balFetcherFunc(func(ctx context.Context, reqs []BALRequest) map[common.Hash]*types.BlockAccessListSidecar {
			balCalls++
			if balCalls == 1 {
				close(started)
				<-ctx.Done()
				err = ctx.Err()
				return nil
			}
			return map[common.Hash]*types.BlockAccessListSidecar{reqs[0].Hash: bal}
		})
		fetcher := &retryingBbdFetcher{balStarted: started}
		bbd := newTestBbd(t, fetcher)
		bbd.peerTracker.PeerConnected(PeerIdFromUint64(2))
		bbd.balFetcher = balFetcher
		withdrawalsHash := empty.RootHash
		header := &types.Header{
			Number:              *uint256.NewInt(1),
			TxHash:              empty.RootHash,
			UncleHash:           empty.UncleHash,
			WithdrawalsHash:     &withdrawalsHash,
			BlockAccessListHash: &balHash,
		}
		feed := BbdResultFeed{ch: make(chan BlockBatchResult, 1)}
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		result := make(chan error, 1)
		go func() {
			result <- bbd.downloadBlocksForHeaders(ctx, []*types.Header{header}, peersContext{}, defaultBbdRequestConfig, ticker, feed)
		}()
		synctest.Wait()
		select {
		case err := <-result:
			require.NoError(t, err)
		default:
			t.Fatal("body retry is still waiting for the failed batch's BAL fetch")
		}
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, ctx.Err())
		require.Equal(t, 2, balCalls)
		require.Len(t, fetcher.peers, 2)
		require.NotEqual(t, fetcher.peers[0], fetcher.peers[1])
		batch, err := feed.Next(ctx)
		require.NoError(t, err)
		require.Len(t, batch.Blocks, 1)
		require.Equal(t, header.Hash(), batch.Blocks[0].Hash())
		require.Same(t, bal, batch.Blocks[0].BlockAccessListSidecar())
	})
}

func newTestHeaderChain(n int, balHash *common.Hash) []*types.Header {
	headers := make([]*types.Header, n)
	parentHash := common.Hash{}
	withdrawalsHash := empty.RootHash
	parentBeaconRoot := common.Hash{}
	requestsHash := common.Hash{}
	var blobGas uint64
	for i := range headers {
		headers[i] = &types.Header{
			Number:                *uint256.NewInt(uint64(i + 1)),
			ParentHash:            parentHash,
			TxHash:                empty.RootHash,
			UncleHash:             empty.UncleHash,
			BaseFee:               uint256.NewInt(1),
			WithdrawalsHash:       &withdrawalsHash,
			BlobGasUsed:           &blobGas,
			ExcessBlobGas:         &blobGas,
			ParentBeaconBlockRoot: &parentBeaconRoot,
			RequestsHash:          &requestsHash,
			BlockAccessListHash:   balHash,
		}
		parentHash = headers[i].Hash()
	}
	return headers
}

func drainBbdFeed(ctx context.Context, t *testing.T, feed BbdResultFeed) int {
	t.Helper()
	var blocks int
	for {
		res, err := feed.Next(ctx)
		require.NoError(t, err)
		if res.Err != nil || res.Blocks == nil {
			require.NoError(t, res.Err)
			return blocks
		}
		blocks += len(res.Blocks)
	}
}

func requireBbdDeliversChain(t *testing.T, fetcher Fetcher, headers []*types.Header, peers uint64) {
	t.Helper()
	logger := testlog.Logger(t, log.LvlCrit)
	peerTracker := NewPeerTracker(logger, nil)
	for i := uint64(1); i <= peers; i++ {
		peerTracker.PeerConnected(PeerIdFromUint64(i))
	}
	bbd := NewBackwardBlockDownloader(logger, NewTrackingFetcher(fetcher, peerTracker), peerTracker, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	feed, err := bbd.DownloadBlocksBackwards(ctx, headers[len(headers)-1].Hash(), fixedHeaderReader{headers[0]}, WithBlocksBatchSize(10))
	require.NoError(t, err)
	require.Equal(t, len(headers)-1, drainBbdFeed(ctx, t, feed))
}

// A persistent full BAL deficit must not fail the download: the blocks are
// delivered BAL-less for bare execution.
func TestBackwardBlockDownloader_DeliversBlocksDespitePersistentBALDeficit(t *testing.T) {
	balHash := common.HexToHash("0xbb")
	headers := newTestHeaderChain(130, &balHash)
	initial := headers[len(headers)-1]
	fetcher := &chainServingBbdFetcher{headers: headers}
	balFetcher := &countingBALFetcher{}
	logger := testlog.Logger(t, log.LvlCrit)
	peerTracker := NewPeerTracker(logger, nil)
	peerTracker.PeerConnected(PeerIdFromUint64(1))
	bbd := NewBackwardBlockDownloader(logger, fetcher, peerTracker, t.TempDir(), WithBALFetcher(balFetcher))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	feed, err := bbd.DownloadBlocksBackwards(ctx, initial.Hash(), fixedHeaderReader{headers[0]})
	require.NoError(t, err)
	require.Equal(t, len(headers)-1, drainBbdFeed(ctx, t, feed))
	require.EqualValues(t, 1, balFetcher.calls.Load())
}

// flakyBodyPeerFetcher drops every other body request sent to each flaky peer,
// so a retry on that peer always succeeds.
type flakyBodyPeerFetcher struct {
	chainServingBbdFetcher
	requests map[PeerId]*atomic.Int64
	pageSize int
}

func newFlakyBodyPeerFetcher(headers []*types.Header, flakyPeers ...uint64) *flakyBodyPeerFetcher {
	requests := make(map[PeerId]*atomic.Int64, len(flakyPeers))
	for _, p := range flakyPeers {
		requests[*PeerIdFromUint64(p)] = &atomic.Int64{}
	}
	return &flakyBodyPeerFetcher{chainServingBbdFetcher: chainServingBbdFetcher{headers: headers}, requests: requests}
}

func (s *flakyBodyPeerFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	pageSize := s.pageSize
	if pageSize == 0 {
		pageSize = len(headers)
	}
	var bodies []*types.Body
	for start := 0; start < len(headers); start += pageSize {
		page := headers[start:min(start+pageSize, len(headers))]
		res, err := fetchWithRetry(defaultFetcherConfig.CopyWithOptions(opts...), func() (*FetcherResponse[[]*types.Body], error) {
			if n, ok := s.requests[*peerId]; ok && n.Add(1)%2 == 1 {
				return nil, context.DeadlineExceeded
			}
			res, err := s.chainServingBbdFetcher.FetchBodies(ctx, page, peerId)
			return &res, err
		})
		if err != nil {
			return FetcherResponse[[]*types.Body]{}, err
		}
		bodies = append(bodies, res.Data...)
	}
	return FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

// Every window waits for its slowest body batch, so a peer that let a body
// request time out must not get a batch in the windows that follow.
func TestBackwardBlockDownloader_BodyTimeoutKeepsPeerOutOfLaterWindows(t *testing.T) {
	headers := newTestHeaderChain(31, nil)
	fetcher := newFlakyBodyPeerFetcher(headers, 1)
	requireBbdDeliversChain(t, fetcher, headers, 3)
	require.EqualValues(t, 1, fetcher.requests[*PeerIdFromUint64(1)].Load())
}

type chainServingBbdFetcher struct {
	stubBbdFetcher
	headers []*types.Header
}

func (s *chainServingBbdFetcher) FetchHeadersBackwards(_ context.Context, hash common.Hash, amount uint64, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	for i, header := range slices.Backward(s.headers) {
		if header.Hash() == hash {
			start := max(i-int(amount)+1, 0)
			return FetcherResponse[[]*types.Header]{Data: s.headers[start : i+1]}, nil
		}
	}
	return FetcherResponse[[]*types.Header]{}, errors.New("unknown header")
}

func (s *chainServingBbdFetcher) FetchBodies(_ context.Context, headers []*types.Header, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	bodies := make([]*types.Body, len(headers))
	for i := range bodies {
		bodies[i] = &types.Body{Withdrawals: types.Withdrawals{}}
	}
	return FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

// EL ahead of the requested header by more than the limit (deep reorg backward): fail-fast at initial-header step.
func TestBackwardBlockDownloader_GapAheadOfCurrentHead_FailsFast(t *testing.T) {
	initialHeader := &types.Header{Number: *uint256.NewInt(100)}
	fetcher := &stubBbdFetcher{initialHeader: initialHeader}
	bbd := newTestBbd(t, fetcher)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(
		ctx, initialHeader.Hash(), stubBbdHeaderReader{},
		WithChainLengthLimit(96),
		WithChainLengthCurrentHead(500),
	)
	require.NoError(t, err)

	_, err = feed.Next(ctx)
	require.ErrorIs(t, err, ErrChainLengthExceedsLimit)
	require.EqualValues(t, 1, fetcher.headersBackwardsCalls.Load(),
		"should fail-fast at the initial-header check without fetching a header batch")
}

// With no other peer to take the batch, a timed-out peer must still get a
// second request, as the per-request retry gave it before.
func TestBackwardBlockDownloader_SinglePeerSurvivesBodyTimeout(t *testing.T) {
	headers := newTestHeaderChain(31, nil)
	fetcher := newFlakyBodyPeerFetcher(headers, 1)
	requireBbdDeliversChain(t, fetcher, headers, 1)
}

// When fewer peers are left than batches, the batches no peer took in a round
// must still be fetched before the window is delivered.
func TestBackwardBlockDownloader_DeliversWholeWindowWhenPeersRunShort(t *testing.T) {
	headers := newTestHeaderChain(31, nil)
	fetcher := newFlakyBodyPeerFetcher(headers, 1, 2)
	requireBbdDeliversChain(t, fetcher, headers, 3)
}

// A batch fetched page by page loses every page when one page fails, so the
// last peer standing must get its per-page retries back.
func TestBackwardBlockDownloader_LastPeerKeepsPerPageRetry(t *testing.T) {
	headers := newTestHeaderChain(11, nil)
	fetcher := newFlakyBodyPeerFetcher(headers, 1)
	fetcher.pageSize = 5
	requireBbdDeliversChain(t, fetcher, headers, 1)
}

type bodyTimeoutFetcher struct {
	chainServingBbdFetcher
	timesOut func(peerId PeerId, fromNum uint64) bool
}

func (s *bodyTimeoutFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	res, err := fetchWithRetry(defaultFetcherConfig.CopyWithOptions(opts...), func() (*FetcherResponse[[]*types.Body], error) {
		if s.timesOut(*peerId, headers[0].Number.Uint64()) {
			return nil, context.DeadlineExceeded
		}
		res, err := s.chainServingBbdFetcher.FetchBodies(ctx, headers, peerId)
		return &res, err
	})
	if err != nil {
		return FetcherResponse[[]*types.Body]{}, err
	}
	return *res, nil
}

// A single body timeout keeps a peer out of the windows that follow, so when
// the remaining peers fail, the last-resort round must try every connected peer.
func TestBackwardBlockDownloader_LastResortRoundTriesPeersMarkedByOldTimeout(t *testing.T) {
	headers := newTestHeaderChain(31, nil)
	timedOutOnce := make(map[PeerId]*atomic.Bool, 4)
	for i := uint64(1); i <= 4; i++ {
		timedOutOnce[*PeerIdFromUint64(i)] = &atomic.Bool{}
	}
	fetcher := &bodyTimeoutFetcher{
		chainServingBbdFetcher: chainServingBbdFetcher{headers: headers},
		timesOut: func(peerId PeerId, fromNum uint64) bool {
			if peerId == *PeerIdFromUint64(5) {
				return fromNum >= 21
			}
			return fromNum >= 11 && fromNum <= 20 && timedOutOnce[peerId].CompareAndSwap(false, true)
		},
	}
	requireBbdDeliversChain(t, fetcher, headers, 5)
	for peerId, timedOut := range timedOutOnce {
		require.True(t, timedOut.Load(), peerId.String())
	}
}
