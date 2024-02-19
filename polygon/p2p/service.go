package p2p

import (
	"context"
	"math/rand"
	"sync"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
)

//go:generate mockgen -destination=./service_mock.go -package=p2p . Service
type Service interface {
	Start(ctx context.Context)
	Stop()
	MaxPeers() int
	PeersSyncProgress() PeersSyncProgress
	DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
	Penalize(ctx context.Context, peerId PeerId) error
}

func NewService(config p2p.Config, logger log.Logger, sentryClient direct.SentryClient) Service {
	return newService(config, logger, sentryClient, rand.Uint64)
}

func newService(
	config p2p.Config,
	logger log.Logger,
	sentryClient direct.SentryClient,
	requestIdGenerator RequestIdGenerator,
) Service {
	messageListener := NewMessageListener(logger, sentryClient)
	messageBroadcaster := NewMessageBroadcaster(sentryClient)
	peerPenalizer := NewPeerPenalizer(sentryClient)
	downloader := NewDownloader(logger, messageListener, messageBroadcaster, peerPenalizer, requestIdGenerator)
	return &service{
		config:          config,
		downloader:      downloader,
		messageListener: messageListener,
		peerPenalizer:   peerPenalizer,
	}
}

type service struct {
	once            sync.Once
	config          p2p.Config
	downloader      Downloader
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
	return s.config.MaxPeers
}

func (s *service) DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	return s.downloader.DownloadHeaders(ctx, start, end, peerId)
}

func (s *service) Penalize(ctx context.Context, peerId PeerId) error {
	return s.peerPenalizer.Penalize(ctx, peerId)
}

func (s *service) PeersSyncProgress() PeersSyncProgress {
	// TODO implement peer tracker
	return nil
}
