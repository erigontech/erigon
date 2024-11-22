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
	"fmt"
	"math/big"

	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

func NewService(logger log.Logger, maxPeers int, sc sentryproto.SentryClient, sdf sentry.StatusDataFactory) *Service {
	peerPenalizer := NewPeerPenalizer(sc)
	messageListener := NewMessageListener(logger, sc, sdf, peerPenalizer)
	peerTracker := NewPeerTracker(logger, sc, messageListener)
	messageSender := NewMessageSender(sc)
	var fetcher Fetcher
	fetcher = NewFetcher(logger, messageListener, messageSender)
	fetcher = NewPenalizingFetcher(logger, fetcher, peerPenalizer)
	fetcher = NewTrackingFetcher(fetcher, peerTracker)
	publisher := NewPublisher(logger, messageSender, peerTracker)
	return &Service{
		logger:          logger,
		fetcher:         fetcher,
		messageListener: messageListener,
		peerPenalizer:   peerPenalizer,
		peerTracker:     peerTracker,
		publisher:       publisher,
		maxPeers:        maxPeers,
	}
}

type Service struct {
	logger          log.Logger
	fetcher         Fetcher
	messageListener *MessageListener
	peerPenalizer   *PeerPenalizer
	peerTracker     *PeerTracker
	publisher       *Publisher
	maxPeers        int
}

func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("[p2p] running p2p service component")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := s.messageListener.Run(ctx); err != nil {
			return fmt.Errorf("message listener failed: %w", err)
		}

		return nil
	})
	eg.Go(func() error {
		if err := s.peerTracker.Run(ctx); err != nil {
			return fmt.Errorf("peer tracker failed: %w", err)
		}

		return nil
	})
	eg.Go(func() error {
		if err := s.publisher.Run(ctx); err != nil {
			return fmt.Errorf("peer Publisher failed: %w", err)
		}

		return nil
	})

	return eg.Wait()
}

func (s *Service) MaxPeers() int {
	return s.maxPeers
}

func (s *Service) ListPeersMayHaveBlockNum(blockNum uint64) []*PeerId {
	return s.peerTracker.ListPeersMayHaveBlockNum(blockNum)
}

func (s *Service) FetchHeaders(ctx context.Context, start, end uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Header], error) {
	return s.fetcher.FetchHeaders(ctx, start, end, peerId, opts...)
}

func (s *Service) FetchBodies(ctx context.Context, headers []*types.Header, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Body], error) {
	return s.fetcher.FetchBodies(ctx, headers, peerId, opts...)
}

func (s *Service) FetchBlocksBackwardsByHash(ctx context.Context, hash libcommon.Hash, amount uint64, peerId *PeerId, opts ...FetcherOption) (FetcherResponse[[]*types.Block], error) {
	return s.fetcher.FetchBlocksBackwardsByHash(ctx, hash, amount, peerId, opts...)
}

func (s *Service) PublishNewBlock(block *types.Block, td *big.Int) {
	s.publisher.PublishNewBlock(block, td)
}

func (s *Service) PublishNewBlockHashes(block *types.Block) {
	s.publisher.PublishNewBlockHashes(block)
}

func (s *Service) Penalize(ctx context.Context, peerId *PeerId) error {
	return s.peerPenalizer.Penalize(ctx, peerId)
}

func (s *Service) RegisterNewBlockObserver(o polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockPacket]]) polygoncommon.UnregisterFunc {
	return s.messageListener.RegisterNewBlockObserver(o)
}

func (s *Service) RegisterNewBlockHashesObserver(o polygoncommon.Observer[*DecodedInboundMessage[*eth.NewBlockHashesPacket]]) polygoncommon.UnregisterFunc {
	return s.messageListener.RegisterNewBlockHashesObserver(o)
}
