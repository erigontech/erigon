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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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
	return NewBackwardBlockDownloader(logger, fetcher, nil, &PeerPenalizer{}, peerTracker, t.TempDir())
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
