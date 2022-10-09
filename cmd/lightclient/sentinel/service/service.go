package service

import (
	"context"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/log/v3"
)

type SentinelServer struct {
	lightrpc.UnimplementedSentinelServer

	ctx            context.Context
	sentinel       *sentinel.Sentinel
	gossipNotifier *gossipNotifier
}

func NewSentinelServer(ctx context.Context, sentinel *sentinel.Sentinel) *SentinelServer {
	return &SentinelServer{
		sentinel:       sentinel,
		ctx:            ctx,
		gossipNotifier: newGossipNotifier(),
	}
}

func (s *SentinelServer) SubscribeGossip(_ *lightrpc.GossipRequest, stream lightrpc.Sentinel_SubscribeGossipServer) error {
	// first of all subscribe
	ch, subId, err := s.gossipNotifier.addSubscriber()
	if err != nil {
		return err
	}
	defer s.gossipNotifier.removeSubscriber(subId)

	for {
		select {
		// Exit on stream context done
		case <-stream.Context().Done():
			return nil
		case packet := <-ch:
			if err := stream.Send(&lightrpc.GossipData{
				Data: packet.data,
				Type: packet.t,
			}); err != nil {
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
	// Compute data
	u := pkt.Packet.(ssz.Marshaler)
	var data []byte
	// Make data
	if data, err = u.MarshalSSZ(); err != nil {
		return err
	}
	switch pkt.Packet.(type) {
	case *lightrpc.SignedBeaconBlockBellatrix:
		s.gossipNotifier.notify(lightrpc.GossipType_BeaconBlockGossipType, data)
	case *lightrpc.LightClientFinalityUpdate:
		s.gossipNotifier.notify(lightrpc.GossipType_LightClientFinalityUpdateGossipType, data)
	case *lightrpc.LightClientOptimisticUpdate:
		s.gossipNotifier.notify(lightrpc.GossipType_LightClientOptimisticUpdateGossipType, data)
	default:
	}
	return nil
}
