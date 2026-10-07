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
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

const trackedHeaderHashes = 1024

func NewTrackingFetcher(fetcher Fetcher, peerTracker *PeerTracker) *TrackingFetcher {
	headerNums, err := simplelru.NewLRU[common.Hash, uint64](trackedHeaderHashes, nil)
	if err != nil {
		panic(err)
	}
	peersMissingHash, err := simplelru.NewLRU[common.Hash, []*PeerId](trackedHeaderHashes, nil)
	if err != nil {
		panic(err)
	}
	return &TrackingFetcher{
		Fetcher:          fetcher,
		peerTracker:      peerTracker,
		headerNums:       headerNums,
		peersMissingHash: peersMissingHash,
	}
}

type TrackingFetcher struct {
	Fetcher
	peerTracker *PeerTracker

	// A peer that lacks a header hash lacks that block number, but a hash request does not say
	// which number it is. The number is learned from whichever peer serves the hash, before or after.
	mu               sync.Mutex
	headerNums       *simplelru.LRU[common.Hash, uint64]
	peersMissingHash *simplelru.LRU[common.Hash, []*PeerId]
}

func (tf *TrackingFetcher) FetchHeaders(
	ctx context.Context,
	start uint64,
	end uint64,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Header], error) {
	res, err := tf.Fetcher.FetchHeaders(ctx, start, end, peerId, opts...)
	if err != nil {
		if errIncompleteHeaders, ok := errors.AsType[*ErrIncompleteHeaders](err); ok {
			tf.peerTracker.BlockNumMissing(peerId, errIncompleteHeaders.LowestMissingBlockNum())
		} else if errors.Is(err, context.DeadlineExceeded) {
			tf.peerTracker.BlockNumMissing(peerId, start)
		}

		return FetcherResponse[[]*types.Header]{}, err
	}

	tf.peerTracker.BlockNumPresent(peerId, res.Data[len(res.Data)-1].Number.Uint64())
	return res, nil
}

func (tf *TrackingFetcher) FetchHeadersBackwards(
	ctx context.Context,
	hash common.Hash,
	amount uint64,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Header], error) {
	res, err := tf.Fetcher.FetchHeadersBackwards(ctx, hash, amount, peerId, opts...)
	if err != nil {
		if errors.Is(err, &ErrMissingHeaderHash{}) {
			tf.headerHashMissing(peerId, hash)
		}
		return FetcherResponse[[]*types.Header]{}, err
	}

	blockNum := res.Data[len(res.Data)-1].Number.Uint64()
	tf.peerTracker.BlockNumPresent(peerId, blockNum)
	tf.headerHashServed(hash, blockNum)
	return res, nil
}

func (tf *TrackingFetcher) headerHashMissing(peerId *PeerId, hash common.Hash) {
	tf.mu.Lock()
	blockNum, known := tf.headerNums.Get(hash)
	if !known {
		peers, _ := tf.peersMissingHash.Get(hash)
		tf.peersMissingHash.Add(hash, append(peers, peerId))
	}
	tf.mu.Unlock()
	if known {
		tf.peerTracker.BlockNumMissing(peerId, blockNum)
	}
}

func (tf *TrackingFetcher) headerHashServed(hash common.Hash, blockNum uint64) {
	tf.mu.Lock()
	tf.headerNums.Add(hash, blockNum)
	peers, _ := tf.peersMissingHash.Peek(hash)
	tf.peersMissingHash.Remove(hash)
	tf.mu.Unlock()
	for _, peerId := range peers {
		tf.peerTracker.BlockNumMissing(peerId, blockNum)
	}
}

func (tf *TrackingFetcher) FetchBodies(
	ctx context.Context,
	headers []*types.Header,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Body], error) {
	bodies, err := tf.Fetcher.FetchBodies(ctx, headers, peerId, opts...)
	if err != nil {
		if errMissingBodies, ok := errors.AsType[*ErrMissingBodies](err); ok {
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
