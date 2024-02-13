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
	DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
	Penalize(ctx context.Context, peerId PeerId) error
}

func NewService(logger log.Logger, sentryClient direct.SentryClient) Service {
	return newService(logger, sentryClient, rand.Uint64)
}

func newService(logger log.Logger, sentryClient direct.SentryClient, requestIdGenerator RequestIdGenerator) Service {
	messageListener := NewMessageListener(logger, sentryClient)
	messageBroadcaster := NewMessageBroadcaster(sentryClient)
	peerManager := NewPeerManager(sentryClient)
	downloader := NewDownloader(logger, messageListener, messageBroadcaster, peerManager, requestIdGenerator)
	return &service{
		downloader:      downloader,
		peerManager:     peerManager,
		messageListener: messageListener,
	}
}

type service struct {
	once            sync.Once
	downloader      Downloader
	peerManager     PeerManager
	messageListener MessageListener
}

func (s *service) Start(ctx context.Context) {
	s.once.Do(func() {
		s.messageListener.Start(ctx)
	})
}

func (s *service) Stop() {
	s.messageListener.Stop()
}

func (s *service) DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	return s.downloader.DownloadHeaders(ctx, start, end, peerId)
}

func (s *service) MaxPeers() int {
	return s.peerManager.MaxPeers()
}

func (s *service) Penalize(ctx context.Context, peerId PeerId) error {
	return s.peerManager.Penalize(ctx, peerId)
}

func (s *service) PeersSyncProgress() PeersSyncProgress {
	return s.peerManager.PeersSyncProgress()
}
