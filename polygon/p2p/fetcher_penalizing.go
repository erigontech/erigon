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
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core/types"
)

func NewPenalizingFetcher(logger log.Logger, fetcher Fetcher, peerPenalizer PeerPenalizer) Fetcher {
	return newPenalizingFetcher(logger, fetcher, peerPenalizer)
}

func newPenalizingFetcher(logger log.Logger, fetcher Fetcher, peerPenalizer PeerPenalizer) *penalizingFetcher {
	fetchHeadersPenalizeErrs := []error{
		&ErrTooManyHeaders{},
		&ErrNonSequentialHeaderNumbers{},
	}

	fetchBodiesPenalizeErrs := []error{
		&ErrTooManyBodies{},
	}

	fetchBlocksPenalizeErrs := make([]error, 0, len(fetchHeadersPenalizeErrs)+len(fetchBodiesPenalizeErrs))
	fetchBlocksPenalizeErrs = append(fetchBlocksPenalizeErrs, fetchHeadersPenalizeErrs...)
	fetchBlocksPenalizeErrs = append(fetchBlocksPenalizeErrs, fetchBodiesPenalizeErrs...)

	return &penalizingFetcher{
		Fetcher:                  fetcher,
		logger:                   logger,
		peerPenalizer:            peerPenalizer,
		fetchHeadersPenalizeErrs: fetchHeadersPenalizeErrs,
		fetchBodiesPenalizeErrs:  fetchBodiesPenalizeErrs,
		fetchBlocksPenalizeErrs:  fetchBlocksPenalizeErrs,
	}
}

type penalizingFetcher struct {
	Fetcher
	logger                   log.Logger
	peerPenalizer            PeerPenalizer
	fetchHeadersPenalizeErrs []error
	fetchBodiesPenalizeErrs  []error
	fetchBlocksPenalizeErrs  []error
}

func (pf *penalizingFetcher) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId *PeerId) (FetcherResponse[[]*types.Header], error) {
	headers, err := pf.Fetcher.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		return FetcherResponse[[]*types.Header]{}, pf.maybePenalize(ctx, peerId, err, pf.fetchHeadersPenalizeErrs...)
	}

	return headers, nil
}

func (pf *penalizingFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId) (FetcherResponse[[]*types.Body], error) {
	bodies, err := pf.Fetcher.FetchBodies(ctx, headers, peerId)
	if err != nil {
		return FetcherResponse[[]*types.Body]{}, pf.maybePenalize(ctx, peerId, err, pf.fetchBodiesPenalizeErrs...)
	}

	return bodies, nil
}

func (pf *penalizingFetcher) FetchBlocks(ctx context.Context, start uint64, end uint64, peerId *PeerId) (FetcherResponse[[]*types.Block], error) {
	blocks, err := pf.Fetcher.FetchBlocks(ctx, start, end, peerId)
	if err != nil {
		return FetcherResponse[[]*types.Block]{}, pf.maybePenalize(ctx, peerId, err, pf.fetchBlocksPenalizeErrs...)
	}

	return blocks, nil
}

func (pf *penalizingFetcher) maybePenalize(ctx context.Context, peerId *PeerId, err error, penalizeErrs ...error) error {
	var shouldPenalize bool
	for _, penalizeErr := range penalizeErrs {
		if errors.Is(err, penalizeErr) {
			shouldPenalize = true
			break
		}
	}

	if shouldPenalize {
		pf.logger.Debug(
			"[p2p.penalizing.fetcher] penalizing peer - penalize-able fetcher issue",
			"peerId", peerId,
			"err", err,
		)

		if penalizeErr := pf.peerPenalizer.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}
	}

	return err
}
