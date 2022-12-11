package network

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
)

type GossipReceiver interface {
	ReceiveGossip(cltypes.ObjectSSZ)
}

type GossipManager struct {
	ctx context.Context

	receivers map[sentinel.GossipType][]GossipReceiver
	sentinel  sentinel.SentinelClient
}

func NewGossipReceiver(ctx context.Context, s sentinel.SentinelClient) *GossipManager {
	return &GossipManager{
		sentinel: s,

		receivers: make(map[sentinel.GossipType][]GossipReceiver),
	}
}

func (g *GossipManager) AddReceiver(t sentinel.GossipType, receiver GossipReceiver) {
	if _, ok := g.receivers[t]; !ok {
		g.receivers[t] = make([]GossipReceiver, 0)
	}
	g.receivers[t] = append(g.receivers[t], receiver)
}

func (g *GossipManager) Start() error {
	subscription, err := g.sentinel.SubscribeGossip(g.ctx, &sentinel.EmptyRequest{})
	if err != nil {
		return err
	}
	go g.loop(subscription)
	return nil
}

func (g *GossipManager) loop(subscription sentinel.Sentinel_SubscribeGossipClient) {
	for {
		data, err := subscription.Recv()
		if err != nil {
			log.Warn("[Beacon Gossip] Failure in receiving", "err", err)
			continue
		}
		receivers := g.receivers[data.Type]
		var object cltypes.ObjectSSZ
		switch data.Type {
		case sentinel.GossipType_BeaconBlockGossipType:
			object = &cltypes.SignedBeaconBlockBellatrix{}
			if err := object.UnmarshalSSZ(data.Data); err != nil {
				log.Warn("[Beacon Gossip] Failure in decoding block", "err", err)
				continue
			}
		case sentinel.GossipType_VoluntaryExitGossipType:
			object = &cltypes.VoluntaryExit{}
			if err := object.UnmarshalSSZ(data.Data); err != nil {
				log.Warn("[Beacon Gossip] Failure in decoding block", "err", err)
				continue
			}
		case sentinel.GossipType_ProposerSlashingGossipType:
			object = &cltypes.ProposerSlashing{}
			if err := object.UnmarshalSSZ(data.Data); err != nil {
				log.Warn("[Beacon Gossip] Failure in decoding block", "err", err)
				continue
			}
		case sentinel.GossipType_AttesterSlashingGossipType:
			object = &cltypes.AttesterSlashing{}
			if err := object.UnmarshalSSZ(data.Data); err != nil {
				log.Warn("[Beacon Gossip] Failure in decoding block", "err", err)
				continue
			}
		case sentinel.GossipType_AggregateAndProofGossipType:
			object = &cltypes.AggregateAndProof{}
			if err := object.UnmarshalSSZ(data.Data); err != nil {
				log.Warn("[Beacon Gossip] Failure in decoding block", "err", err)
				continue
			}

		}
		// If we received a valid object give it to our receiver
		if object != nil {
			for _, receiver := range receivers {
				receiver.ReceiveGossip(object)
			}
		}
	}
}
