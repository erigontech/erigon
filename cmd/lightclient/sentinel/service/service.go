package service

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/log/v3"
)

type SentinelServer struct {
	lightrpc.UnimplementedSentinelServer

	ctx            context.Context
	sentinel       *sentinel.Sentinel
	gossipNotifier gossipNotifier
}

func NewSentinelServer(ctx context.Context, sentinel *sentinel.Sentinel) *SentinelServer {
	return &SentinelServer{
		sentinel:       sentinel,
		ctx:            ctx,
		gossipNotifier: newGossipNotifier(),
	}
}

func (s *SentinelServer) SubscribeBeaconBlock(_ *lightrpc.GossipRequest, stream lightrpc.Sentinel_SubscribeBeaconBlockServer) error {
	// first of all subscribe
	ch, subId, err := s.gossipNotifier.addSubscriber(beaconBlock)
	if err != nil {
		return err
	}
	defer s.gossipNotifier.removeSubscriber(beaconBlock, subId)

	for {
		select {
		// Exit on stream context done
		case <-stream.Context().Done():
			return nil
		case packet := <-ch:
			err = stream.Send(packet.(*lightrpc.SignedBeaconBlockBellatrix))
			if err != nil {
				log.Warn("Could not relay gossip packet", "reason", err)
			}
		}
	}
}

func (s *SentinelServer) ListenToGossip() {
	for {
		select {
		case pkt := <-s.sentinel.RecvGossip():
			s.handleGossipPacket(pkt)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *SentinelServer) handleGossipPacket(pkt *communication.GossipContext) error {
	log.Trace("[Gossip] Received Packet", "topic", pkt.Topic)
	err := pkt.Codec.WritePacket(context.TODO(), pkt.Packet)
	if err != nil {
		log.Warn("[Gossip] Error Forwarding Packet", "err", err)
	}
	switch pkt.Packet.(type) {
	case *lightrpc.SignedBeaconBlockBellatrix:
		s.gossipNotifier.notify(beaconBlock, pkt.Packet)
	case *lightrpc.LightClientFinalityUpdate:
		s.gossipNotifier.notify(lightClientFinalityUpdate, pkt.Packet)
	case *lightrpc.LightClientOptimisticUpdate:
		s.gossipNotifier.notify(lightClientOptimisticUpdate, pkt.Packet)
	default:
	}
	return nil
}
