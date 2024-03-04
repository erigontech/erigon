package p2p

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

func NewPenalizingFetcher(logger log.Logger, fetcher Fetcher, peerPenalizer PeerPenalizer) Fetcher {
	return &penalizingFetcher{
		Fetcher:       fetcher,
		logger:        logger,
		peerPenalizer: peerPenalizer,
	}
}

type penalizingFetcher struct {
	Fetcher
	logger        log.Logger
	peerPenalizer PeerPenalizer
}

func (pf *penalizingFetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	headers, err := pf.Fetcher.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		shouldPenalize := rlp.IsInvalidRLPError(err) ||
			errors.Is(err, &ErrTooManyHeaders{}) ||
			errors.Is(err, &ErrNonSequentialHeaderNumbers{})

		if shouldPenalize {
			pf.logger.Debug("penalizing peer", "peerId", peerId, "err", err.Error())

			penalizeErr := pf.peerPenalizer.Penalize(ctx, peerId)
			if penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, err
	}

	return headers, nil
}
