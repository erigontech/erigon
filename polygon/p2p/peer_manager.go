package p2p

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

type PeerManager interface {
	MaxPeers() int
	PeersSyncProgress() PeersSyncProgress
	Penalize(ctx context.Context, peerId PeerId) error
}

func NewPeerManager(sentryClient direct.SentryClient) PeerManager {
	return &peerManager{
		sentryClient: sentryClient,
	}
}

type peerManager struct {
	sentryClient direct.SentryClient
}

func (pm *peerManager) MaxPeers() int {
	//TODO implement me - should return max peers from p2p config
	panic("implement me")
}

func (pm *peerManager) PeersSyncProgress() PeersSyncProgress {
	//TODO implement me
	panic("implement me")
}

func (pm *peerManager) Penalize(ctx context.Context, peerId PeerId) error {
	_, err := pm.sentryClient.PenalizePeer(ctx, &sentry.PenalizePeerRequest{
		PeerId:  peerId.H512(),
		Penalty: sentry.PenaltyKind_Kick,
	})

	return err
}
