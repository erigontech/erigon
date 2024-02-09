package p2p

import (
	"context"
	"math/rand"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/types"
)

//go:generate mockgen -destination=./service_mock.go -package=p2p . Service
type Service interface {
	MaxPeers() int
	PeersSyncProgress() PeersSyncProgress
	DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error)
	Penalize(peerId PeerId)
}

func NewService(ctx context.Context, logger log.Logger, sentryClient direct.SentryClient) Service {
	return newService(ctx, logger, sentryClient, rand.Uint64)
}

func newService(
	ctx context.Context,
	logger log.Logger,
	sentryClient direct.SentryClient,
	requestIdGenerator requestIdGenerator,
) Service {
	messageListener := &messageListener{
		logger:       logger,
		sentryClient: sentryClient,
		observers:    map[sentry.MessageId]map[messageObserver]struct{}{},
	}

	messageListener.Listen(ctx)

	messageBroadcaster := &messageBroadcaster{
		sentryClient: sentryClient,
	}

	downloader := &downloader{
		messageListener:    messageListener,
		messageBroadcaster: messageBroadcaster,
		requestIdGenerator: requestIdGenerator,
	}

	peerManager := &peerManager{}

	return &service{
		downloader:  downloader,
		peerManager: peerManager,
	}
}

type service struct {
	downloader  *downloader
	peerManager *peerManager
}

func (s service) DownloadHeaders(ctx context.Context, start uint64, end uint64, peerId PeerId) ([]*types.Header, error) {
	return s.downloader.DownloadHeaders(ctx, start, end, peerId)
}

func (s service) MaxPeers() int {
	return s.peerManager.MaxPeers()
}

func (s service) Penalize(peerId PeerId) {
	s.peerManager.Penalize(peerId)
}

func (s service) PeersSyncProgress() PeersSyncProgress {
	return s.peerManager.PeersSyncProgress()
}
