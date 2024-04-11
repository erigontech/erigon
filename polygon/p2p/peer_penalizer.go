package p2p

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

type PeerPenalizer interface {
	Penalize(ctx context.Context, peerId *PeerId) error
}

func NewTrackingPeerPenalizer(peerPenalizer PeerPenalizer, peerTracker PeerTracker) PeerPenalizer {
	return &trackingPeerPenalizer{
		PeerPenalizer: peerPenalizer,
		peerTracker:   peerTracker,
	}
}

type trackingPeerPenalizer struct {
	PeerPenalizer
	peerTracker PeerTracker
}

func (p *trackingPeerPenalizer) Penalize(ctx context.Context, peerId *PeerId) error {
	p.peerTracker.PeerDisconnected(peerId)
	return p.PeerPenalizer.Penalize(ctx, peerId)
}

func NewPeerPenalizer(sentryClient direct.SentryClient) PeerPenalizer {
	return &peerPenalizer{
		sentryClient: sentryClient,
	}
}

type peerPenalizer struct {
	sentryClient direct.SentryClient
}

func (p *peerPenalizer) Penalize(ctx context.Context, peerId *PeerId) error {
	_, err := p.sentryClient.PenalizePeer(ctx, &sentry.PenalizePeerRequest{
		PeerId:  peerId.H512(),
		Penalty: sentry.PenaltyKind_Kick,
	})

	return err
}
