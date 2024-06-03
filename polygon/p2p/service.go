package p2p

import (
	"math/rand"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/direct"
	sentrymulticlient "github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
)

//go:generate mockgen -typed=true -source=./service.go -destination=./service_mock.go -package=p2p . Service
type Service interface {
	Fetcher
	MessageListener
	PeerTracker
	PeerPenalizer
	MaxPeers() int
}

func NewService(
	maxPeers int,
	logger log.Logger,
	sentryClient direct.SentryClient,
	statusDataFactory sentrymulticlient.StatusDataFactory,
) Service {
	fetcherConfig := FetcherConfig{
		responseTimeout: 5 * time.Second,
		retryBackOff:    10 * time.Second,
		maxRetries:      2,
	}

	return newService(maxPeers, fetcherConfig, logger, sentryClient, statusDataFactory, rand.Uint64)
}

func newService(
	maxPeers int,
	fetcherConfig FetcherConfig,
	logger log.Logger,
	sentryClient direct.SentryClient,
	statusDataFactory sentrymulticlient.StatusDataFactory,
	requestIdGenerator RequestIdGenerator,
) *service {
	peerTracker := NewPeerTracker()
	peerPenalizer := NewPeerPenalizer(sentryClient)
	messageListener := NewMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer)
	messageListener.RegisterPeerEventObserver(NewPeerEventObserver(logger, peerTracker))
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

func (s *service) MaxPeers() int {
	return s.maxPeers
}
