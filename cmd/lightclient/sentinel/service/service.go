package service

import (
	"context"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
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

func (s *SentinelServer) SubscribeGossip(_ *lightrpc.EmptyRequest, stream lightrpc.Sentinel_SubscribeGossipServer) error {
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

func (s *SentinelServer) SendRequest(_ context.Context, req *lightrpc.RequestData) (*lightrpc.ResponseData, error) {
	// Send the request and get the data if we get an answer.
	respData, foundErrReq, err := s.sentinel.SendRequestRaw(req.Data, req.Topic)
	return &lightrpc.ResponseData{
		Data:  respData,
		Error: foundErrReq,
	}, err
}

func (s *SentinelServer) GetPeers(_ context.Context, _ *lightrpc.EmptyRequest) (*lightrpc.PeerCount, error) {
	// Send the request and get the data if we get an answer.
	return &lightrpc.PeerCount{
		Amount: uint64(s.sentinel.GetPeersCount()),
	}, nil
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
	case *cltypes.SignedBeaconBlockBellatrix:
		s.gossipNotifier.notify(lightrpc.GossipType_BeaconBlockGossipType, data)
	case *cltypes.LightClientFinalityUpdate:
		s.gossipNotifier.notify(lightrpc.GossipType_LightClientFinalityUpdateGossipType, data)
	case *cltypes.LightClientOptimisticUpdate:
		s.gossipNotifier.notify(lightrpc.GossipType_LightClientOptimisticUpdateGossipType, data)
	default:
	}
	return nil
}
