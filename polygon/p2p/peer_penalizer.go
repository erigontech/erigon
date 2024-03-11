package p2p

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

func NewPeerPenalizer(sentryClient direct.SentryClient) PeerPenalizer {
	return &peerPenalizer{
		sentryClient: sentryClient,
	}
}

type PeerPenalizer interface {
	Penalize(ctx context.Context, peerId *PeerId) error
}

type peerPenalizer struct {
	sentryClient direct.SentryClient
}

func (pp *peerPenalizer) Penalize(ctx context.Context, peerId *PeerId) error {
	_, err := pp.sentryClient.PenalizePeer(ctx, &sentry.PenalizePeerRequest{
		PeerId:  peerId.H512(),
		Penalty: sentry.PenaltyKind_Kick,
	})

	return err
}
