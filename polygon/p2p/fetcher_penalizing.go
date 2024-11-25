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

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/core/types"
)

func NewPenalizingFetcher(logger log.Logger, fetcher Fetcher, peerPenalizer *PeerPenalizer) *PenalizingFetcher {
	fetchHeadersPenalizeErrs := []error{
		&ErrTooManyHeaders{},
		&ErrNonSequentialHeaderNumbers{},
		&ErrNonSequentialHeaderHashes{},
	}

	fetchBodiesPenalizeErrs := []error{
		&ErrTooManyBodies{},
		&ErrMissingBodies{},
	}

	fetchBlocksBackwardsByHashPenalizeErrs := append([]error{}, fetchHeadersPenalizeErrs...)
	fetchBlocksBackwardsByHashPenalizeErrs = append(fetchBlocksBackwardsByHashPenalizeErrs, &ErrUnexpectedHeaderHash{})
	fetchBlocksBackwardsByHashPenalizeErrs = append(fetchBlocksBackwardsByHashPenalizeErrs, fetchBodiesPenalizeErrs...)

	return &PenalizingFetcher{
		Fetcher:                                fetcher,
		logger:                                 logger,
		peerPenalizer:                          peerPenalizer,
		fetchHeadersPenalizeErrs:               fetchHeadersPenalizeErrs,
		fetchBodiesPenalizeErrs:                fetchBodiesPenalizeErrs,
		fetchBlocksBackwardsByHashPenalizeErrs: fetchBlocksBackwardsByHashPenalizeErrs,
	}
}

type PenalizingFetcher struct {
	Fetcher
	logger                                 log.Logger
	peerPenalizer                          *PeerPenalizer
	fetchHeadersPenalizeErrs               []error
	fetchBodiesPenalizeErrs                []error
	fetchBlocksBackwardsByHashPenalizeErrs []error
}

func (pf *PenalizingFetcher) FetchHeaders(
	ctx context.Context,
	start uint64,
	end uint64,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Header], error) {
	headers, err := pf.Fetcher.FetchHeaders(ctx, start, end, peerId, opts...)
	if err != nil {
		return FetcherResponse[[]*types.Header]{}, pf.maybePenalize(ctx, peerId, err, pf.fetchHeadersPenalizeErrs...)
	}

	return headers, nil
}

func (pf *PenalizingFetcher) FetchBodies(
	ctx context.Context,
	headers []*types.Header,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Body], error) {
	bodies, err := pf.Fetcher.FetchBodies(ctx, headers, peerId, opts...)
	if err != nil {
		return FetcherResponse[[]*types.Body]{}, pf.maybePenalize(ctx, peerId, err, pf.fetchBodiesPenalizeErrs...)
	}

	return bodies, nil
}

func (pf *PenalizingFetcher) FetchBlocksBackwardsByHash(
	ctx context.Context,
	hash common.Hash,
	amount uint64,
	peerId *PeerId,
	opts ...FetcherOption,
) (FetcherResponse[[]*types.Block], error) {
	blocks, err := pf.Fetcher.FetchBlocksBackwardsByHash(ctx, hash, amount, peerId, opts...)
	if err != nil {
		err = pf.maybePenalize(ctx, peerId, err, pf.fetchBlocksBackwardsByHashPenalizeErrs...)
		return FetcherResponse[[]*types.Block]{}, err
	}

	return blocks, nil
}

func (pf *PenalizingFetcher) maybePenalize(ctx context.Context, peerId *PeerId, err error, penalizeErrs ...error) error {
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
