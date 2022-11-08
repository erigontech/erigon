package service

import (
	"context"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/log/v3"
)

type SentinelServer struct {
	consensusrpc.UnimplementedSentinelServer

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

func (s *SentinelServer) SubscribeGossip(_ *consensusrpc.EmptyRequest, stream consensusrpc.Sentinel_SubscribeGossipServer) error {
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
			if err := stream.Send(&consensusrpc.GossipData{
				Data: packet.data,
				Type: packet.t,
			}); err != nil {
				log.Warn("Could not relay gossip packet", "reason", err)
			}
		}
	}
}

func (s *SentinelServer) SendRequest(_ context.Context, req *consensusrpc.RequestData) (*consensusrpc.ResponseData, error) {
	// Send the request and get the data if we get an answer.
	respData, foundErrReq, err := s.sentinel.SendRequestRaw(req.Data, req.Topic)
	return &consensusrpc.ResponseData{
		Data:  respData,
		Error: foundErrReq,
	}, err
}

func (s *SentinelServer) GetPeers(_ context.Context, _ *consensusrpc.EmptyRequest) (*consensusrpc.PeerCount, error) {
	// Send the request and get the data if we get an answer.
	return &consensusrpc.PeerCount{
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
		s.gossipNotifier.notify(consensusrpc.GossipType_BeaconBlockGossipType, data)
	case *cltypes.SignedAggregateAndProof:
		s.gossipNotifier.notify(consensusrpc.GossipType_AggregateAndProofGossipType, data)
	case *cltypes.SignedVoluntaryExit:
		s.gossipNotifier.notify(consensusrpc.GossipType_VoluntaryExitGossipType, data)
	case *cltypes.ProposerSlashing:
		s.gossipNotifier.notify(consensusrpc.GossipType_ProposerSlashingGossipType, data)
	case *cltypes.AttesterSlashing:
		s.gossipNotifier.notify(consensusrpc.GossipType_AttesterSlashingGossipType, data)
	case *cltypes.LightClientFinalityUpdate:
		s.gossipNotifier.notify(consensusrpc.GossipType_LightClientFinalityUpdateGossipType, data)
	case *cltypes.LightClientOptimisticUpdate:
		s.gossipNotifier.notify(consensusrpc.GossipType_LightClientOptimisticUpdateGossipType, data)
	default:
	}
	return nil
}
