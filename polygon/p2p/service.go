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

package polygonp2p

import (
	"context"
	"fmt"
	"math/big"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

func NewService(logger log.Logger, maxPeers int, sc sentryproto.SentryClient, sdf libsentry.StatusDataFactory, tmpDir string) *Service {
	peerPenalizer := p2p.NewPeerPenalizer(sc)
	messageListener := p2p.NewMessageListener(logger, sc, sdf, peerPenalizer)
	peerTracker := p2p.NewPeerTracker(logger, messageListener)
	messageSender := p2p.NewMessageSender(sc)
	var fetcher p2p.Fetcher
	fetcher = p2p.NewFetcher(logger, messageListener, messageSender)
	fetcher = p2p.NewPenalizingFetcher(logger, fetcher, peerPenalizer)
	fetcher = p2p.NewTrackingFetcher(fetcher, peerTracker)
	publisher := p2p.NewPublisher(logger, messageSender, peerTracker)
	bbd := p2p.NewBackwardBlockDownloader(logger, fetcher, peerPenalizer, peerTracker, tmpDir)
	return &Service{
		logger:          logger,
		fetcher:         fetcher,
		messageListener: messageListener,
		peerPenalizer:   peerPenalizer,
		peerTracker:     peerTracker,
		publisher:       publisher,
		bbd:             bbd,
		maxPeers:        maxPeers,
	}
}

type Service struct {
	logger          log.Logger
	fetcher         p2p.Fetcher
	messageListener *p2p.MessageListener
	peerPenalizer   *p2p.PeerPenalizer
	peerTracker     *p2p.PeerTracker
	publisher       *p2p.Publisher
	bbd             *p2p.BackwardBlockDownloader
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

func (s *Service) ListPeersMayHaveBlockNum(blockNum uint64) []*p2p.PeerId {
	return s.peerTracker.ListPeersMayHaveBlockNum(blockNum)
}

func (s *Service) FetchHeaders(ctx context.Context, start, end uint64, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	return s.fetcher.FetchHeaders(ctx, start, end, peerId, opts...)
}

func (s *Service) FetchBodies(ctx context.Context, headers []*types.Header, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	return s.fetcher.FetchBodies(ctx, headers, peerId, opts...)
}

func (s *Service) FetchBlocksBackwards(ctx context.Context, hash common.Hash, hr p2p.BbdHeaderReader, opts ...p2p.BbdOption) (p2p.BbdResultFeed, error) {
	return s.bbd.DownloadBlocksBackwards(ctx, hash, hr, opts...)
}

func (s *Service) PublishNewBlock(block *types.Block, td *big.Int) {
	s.publisher.PublishNewBlock(block, td)
}

func (s *Service) PublishNewBlockHashes(block *types.Block) {
	s.publisher.PublishNewBlockHashes(block)
}

func (s *Service) Penalize(ctx context.Context, peerId *p2p.PeerId) error {
	return s.peerPenalizer.Penalize(ctx, peerId)
}

func (s *Service) RegisterNewBlockObserver(o event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]) event.UnregisterFunc {
	return s.messageListener.RegisterNewBlockObserver(o)
}

func (s *Service) RegisterNewBlockHashesObserver(o event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]) event.UnregisterFunc {
	return s.messageListener.RegisterNewBlockHashesObserver(o)
}
