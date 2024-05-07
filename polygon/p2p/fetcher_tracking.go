package p2p

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon/core/types"
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

func (tf *trackingFetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId *PeerId) ([]*types.Header, error) {
	res, err := tf.Fetcher.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		var errIncompleteHeaders *ErrIncompleteHeaders
		if errors.As(err, &errIncompleteHeaders) {
			tf.peerTracker.BlockNumMissing(peerId, errIncompleteHeaders.LowestMissingBlockNum())
		} else if errors.Is(err, context.DeadlineExceeded) {
			tf.peerTracker.BlockNumMissing(peerId, start)
		}

		return nil, err
	}

	tf.peerTracker.BlockNumPresent(peerId, res[len(res)-1].Number.Uint64())
	return res, nil
}

func (tf *trackingFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId) ([]*types.Body, error) {
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

		return nil, err
	}

	return bodies, nil
}
