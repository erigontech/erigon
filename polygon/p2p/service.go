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
	"math/rand"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/p2p/sentry"
)

//go:generate mockgen -typed=true -source=./service.go -destination=./service_mock.go -package=p2p . Service
type Service interface {
	Fetcher
	MessageListener
	PeerTracker
	PeerPenalizer
	Run(ctx context.Context) error
	MaxPeers() int
}

func NewService(
	maxPeers int,
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory sentry.StatusDataFactory,
) Service {
	return newService(maxPeers, defaultFetcherConfig, logger, sentryClient, statusDataFactory, rand.Uint64)
}

func newService(
	maxPeers int,
	fetcherConfig FetcherConfig,
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory sentry.StatusDataFactory,
	requestIdGenerator RequestIdGenerator,
) *service {
	peerPenalizer := NewPeerPenalizer(sentryClient)
	messageListener := NewMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer)
	peerTracker := NewPeerTracker(logger, sentryClient, messageListener)
	messageSender := NewMessageSender(sentryClient)
	fetcher := NewFetcher(fetcherConfig, messageListener, messageSender, requestIdGenerator)
	fetcher = NewPenalizingFetcher(logger, fetcher, peerPenalizer)
	fetcher = NewTrackingFetcher(fetcher, peerTracker)
	return &service{
		Fetcher:         fetcher,
		MessageListener: messageListener,
		PeerPenalizer:   peerPenalizer,
		PeerTracker:     peerTracker,
		maxPeers:        maxPeers,
	}
}

type service struct {
	Fetcher
	MessageListener
	PeerPenalizer
	PeerTracker
	maxPeers int
}

func (s *service) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.MessageListener.Run(ctx) })
	eg.Go(func() error { return s.PeerTracker.Run(ctx) })
	return eg.Wait()
}

func (s *service) MaxPeers() int {
	return s.maxPeers
}
