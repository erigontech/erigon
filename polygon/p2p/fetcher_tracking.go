package p2p

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon/core/types"
)

func NewTrackingFetcher(fetcher Fetcher, peerTracker PeerTracker) Fetcher {
	return &trackingFetcher{
		Fetcher:     fetcher,
		peerTracker: peerTracker,
	}
}

type trackingFetcher struct {
	Fetcher
	peerTracker PeerTracker
}

func (tf *trackingFetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	res, err := tf.Fetcher.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		var errIncompleteResponse *ErrIncompleteHeaders
		if errors.As(err, &errIncompleteResponse) {
			tf.peerTracker.BlockNumMissing(peerId, errIncompleteResponse.LowestMissingBlockNum())
		} else if errors.Is(err, context.DeadlineExceeded) {
			tf.peerTracker.BlockNumMissing(peerId, start)
		}

		return nil, err
	}

	tf.peerTracker.BlockNumPresent(peerId, res[len(res)-1].Number.Uint64())
	return res, nil
}
