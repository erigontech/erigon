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

package p2p

import (
	"context"
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
)

func NewTrackingFetcher(fetcher Fetcher, peerTracker PeerTracker) Fetcher {
	return newTrackingFetcher(fetcher, peerTracker)
}

func newTrackingFetcher(fetcher Fetcher, peerTracker PeerTracker) *trackingFetcher {
	return &trackingFetcher{
		Fetcher:     fetcher,
		peerTracker: peerTracker,
	}
}

type trackingFetcher struct {
	Fetcher
	peerTracker PeerTracker
}

func (tf *trackingFetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId *PeerId) (FetcherResponse[[]*types.Header], error) {
	res, err := tf.Fetcher.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		var errIncompleteHeaders *ErrIncompleteHeaders
		if errors.As(err, &errIncompleteHeaders) {
			tf.peerTracker.BlockNumMissing(peerId, errIncompleteHeaders.LowestMissingBlockNum())
		} else if errors.Is(err, context.DeadlineExceeded) {
			tf.peerTracker.BlockNumMissing(peerId, start)
		}

		return FetcherResponse[[]*types.Header]{}, err
	}

	tf.peerTracker.BlockNumPresent(peerId, res.Data[len(res.Data)-1].Number.Uint64())
	return res, nil
}

func (tf *trackingFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId) (FetcherResponse[[]*types.Body], error) {
	bodies, err := tf.Fetcher.FetchBodies(ctx, headers, peerId)
	if err != nil {
		var errMissingBodies *ErrMissingBodies
		if errors.As(err, &errMissingBodies) {
			lowest, exists := errMissingBodies.LowestMissingBlockNum()
			if exists {
				tf.peerTracker.BlockNumMissing(peerId, lowest)
			}
		} else if errors.Is(err, context.DeadlineExceeded) {
			lowest, exists := lowestHeadersNum(headers)
			if exists {
				tf.peerTracker.BlockNumMissing(peerId, lowest)
			}
		}

		return FetcherResponse[[]*types.Body]{}, err
	}

	return bodies, nil
}

func (tf *trackingFetcher) FetchBlock(
	ctx context.Context,
	hash common.Hash,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[*types.Block], error) {
	block, err := tf.Fetcher.FetchBlockByHash(ctx, hash, peerId, opts...)
	if err != nil {
		return FetcherResponse[*types.Block]{}, err
	}

	tf.peerTracker.BlockNumPresent(peerId, block.Data.NumberU64())
	return block, nil
}
