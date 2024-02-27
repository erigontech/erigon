package p2p

import (
	"context"
	"math/rand"
	"sync"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./service_mock.go -package=p2p . Service
type Service interface {
	Start(ctx context.Context)
	Stop()
	MaxPeers() int
	PeersSyncProgress() PeersSyncProgress
	// FetchHeaders fetches [start,end) headers from a peer. Blocks until data is received.
	FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
	Penalize(ctx context.Context, peerId PeerId) error
}

func NewService(maxPeers int, logger log.Logger, sentryClient direct.SentryClient) Service {
	return newService(maxPeers, logger, sentryClient, rand.Uint64)
}

func newService(
	maxPeers int,
	logger log.Logger,
	sentryClient direct.SentryClient,
	requestIdGenerator RequestIdGenerator,
) Service {
	messageListener := NewMessageListener(logger, sentryClient)
	messageSender := NewMessageSender(sentryClient)
	peerPenalizer := NewPeerPenalizer(sentryClient)
	fetcher := NewFetcher(logger, messageListener, messageSender, peerPenalizer, requestIdGenerator)
	return &service{
		maxPeers:        maxPeers,
		fetcher:         fetcher,
		messageListener: messageListener,
		peerPenalizer:   peerPenalizer,
	}
}

type service struct {
	once            sync.Once
	maxPeers        int
	fetcher         Fetcher
	messageListener MessageListener
	peerPenalizer   PeerPenalizer
}

func (s *service) Start(ctx context.Context) {
	s.once.Do(func() {
		s.messageListener.Start(ctx)
	})
}

func (s *service) Stop() {
	s.messageListener.Stop()
}

func (s *service) MaxPeers() int {
	return s.maxPeers
}

func (s *service) FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	return s.fetcher.FetchHeaders(ctx, start, end, peerId)
}

func (s *service) Penalize(ctx context.Context, peerId PeerId) error {
	return s.peerPenalizer.Penalize(ctx, peerId)
}

func (s *service) PeersSyncProgress() PeersSyncProgress {
	// TODO implement peer tracker
	return nil
}
