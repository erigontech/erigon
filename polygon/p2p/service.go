package p2p

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./service_mock.go -package=p2p . Service
type Service interface {
	Start(ctx context.Context)
	Stop()
	MaxPeers() int
	ListPeersMayHaveBlockNum(blockNum uint64) []PeerId
	// FetchHeaders fetches [start,end) headers from a peer. Blocks until data is received.
	FetchHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
	Penalize(ctx context.Context, peerId PeerId) error
	GetMessageListener() MessageListener
}

func NewService(maxPeers int, logger log.Logger, sentryClient direct.SentryClient) Service {
	fetcherConfig := FetcherConfig{
		responseTimeout: 5 * time.Second,
		retryBackOff:    10 * time.Second,
		maxRetries:      2,
	}

	return newService(maxPeers, fetcherConfig, logger, sentryClient, rand.Uint64)
}

func newService(
	maxPeers int,
	fetcherConfig FetcherConfig,
	logger log.Logger,
	sentryClient direct.SentryClient,
	requestIdGenerator RequestIdGenerator,
) Service {
	peerTracker := NewPeerTracker()
	messageListener := NewMessageListener(logger, sentryClient)
	messageListener.RegisterPeerEventObserver(NewPeerEventObserver(peerTracker))
	messageSender := NewMessageSender(sentryClient)
	peerPenalizer := NewPeerPenalizer(sentryClient)
	fetcher := NewFetcher(fetcherConfig, logger, messageListener, messageSender, requestIdGenerator)
	fetcher = NewPenalizingFetcher(logger, fetcher, peerPenalizer)
	fetcher = NewTrackingFetcher(fetcher, peerTracker)
	return &service{
		maxPeers:        maxPeers,
		fetcher:         fetcher,
		messageListener: messageListener,
		peerPenalizer:   peerPenalizer,
		peerTracker:     peerTracker,
		logger:          logger,
	}
}

type service struct {
	once            sync.Once
	maxPeers        int
	fetcher         Fetcher
	messageListener MessageListener
	peerPenalizer   PeerPenalizer
	peerTracker     PeerTracker
	logger          log.Logger
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

func (s *service) ListPeersMayHaveBlockNum(blockNum uint64) []PeerId {
	return s.peerTracker.ListPeersMayHaveBlockNum(blockNum)
}

func (s *service) GetMessageListener() MessageListener {
	return s.messageListener
}
