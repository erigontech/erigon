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
	"sync/atomic"
	"testing"
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
	return NewBackwardBlockDownloader(logger, fetcher, &PeerPenalizer{}, peerTracker, t.TempDir())
}

// EL behind the requested header by more than the limit: fail-fast at initial-header step.
func TestBackwardBlockDownloader_GapBehindCurrentHead_FailsFast(t *testing.T) {
	initialHeader := &types.Header{Number: *uint256.NewInt(500)}
	fetcher := &stubBbdFetcher{initialHeader: initialHeader}
	bbd := newTestBbd(t, fetcher)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	feed, err := bbd.DownloadBlocksBackwards(ctx, initialHeader.Hash(), stubBbdHeaderReader{},
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

func (s *countingBALFetcher) Fetch(_ context.Context, reqs []BALRequest, _ *PeerId, _ []PeerId, _ time.Duration, _ time.Duration) map[common.Hash][]byte {
	s.calls.Add(1)
	return nil
}

// A persistent full BAL deficit must not fail the download: the blocks are
// delivered BAL-less for bare execution.
func TestBackwardBlockDownloader_DeliversBlocksDespitePersistentBALDeficit(t *testing.T) {
	headers := make([]*types.Header, 130)
	parentHash := common.Hash{}
	withdrawalsHash := empty.RootHash
	parentBeaconRoot := common.Hash{}
	requestsHash := common.Hash{}
	var blobGas uint64
	balHash := common.HexToHash("0xbb")
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
			BlockAccessListHash:   &balHash,
		}
		parentHash = headers[i].Hash()
	}
	initial := headers[len(headers)-1]
	fetcher := &chainServingBbdFetcher{headers: headers}
	balFetcher := &countingBALFetcher{}
	logger := testlog.Logger(t, log.LvlCrit)
	peerTracker := NewPeerTracker(logger, nil)
	peerTracker.PeerConnected(PeerIdFromUint64(1))
	bbd := NewBackwardBlockDownloader(logger, fetcher, &PeerPenalizer{}, peerTracker, t.TempDir(), WithBALFetcher(balFetcher))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	feed, err := bbd.DownloadBlocksBackwards(ctx, initial.Hash(), fixedHeaderReader{headers[0]})
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
	require.EqualValues(t, 1, balFetcher.calls.Load())
}

type chainServingBbdFetcher struct {
	stubBbdFetcher
	headers []*types.Header
}

func (s *chainServingBbdFetcher) FetchHeadersBackwards(_ context.Context, hash common.Hash, amount uint64, _ *PeerId, _ ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	for i := len(s.headers) - 1; i >= 0; i-- {
		if s.headers[i].Hash() == hash {
			start := i - int(amount) + 1
			if start < 0 {
				start = 0
			}
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

	feed, err := bbd.DownloadBlocksBackwards(ctx, initialHeader.Hash(), stubBbdHeaderReader{},
		WithChainLengthLimit(96),
		WithChainLengthCurrentHead(500),
	)
	require.NoError(t, err)

	_, err = feed.Next(ctx)
	require.ErrorIs(t, err, ErrChainLengthExceedsLimit)
	require.EqualValues(t, 1, fetcher.headersBackwardsCalls.Load(),
		"should fail-fast at the initial-header check without fetching a header batch")
}
