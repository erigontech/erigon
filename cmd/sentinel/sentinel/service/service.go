package service

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/log/v3"
	ssz "github.com/prysmaticlabs/fastssz"
)

type SentinelServer struct {
	sentinelrpc.UnimplementedSentinelServer

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

func (s *SentinelServer) SubscribeGossip(_ *sentinelrpc.EmptyMessage, stream sentinelrpc.Sentinel_SubscribeGossipServer) error {
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
			if err := stream.Send(&sentinelrpc.GossipData{
				Data: packet.data,
				Type: packet.t,
			}); err != nil {
				log.Warn("[Sentinel] Could not relay gossip packet", "reason", err)
			}
		}
	}
}

func (s *SentinelServer) SendRequest(_ context.Context, req *sentinelrpc.RequestData) (*sentinelrpc.ResponseData, error) {
	retryReqInterval := time.NewTicker(10 * time.Millisecond)
	defer retryReqInterval.Stop()
	doneCh := make(chan *sentinelrpc.ResponseData)
	// Try finding the data to our peers
	for {
		select {
		case <-s.ctx.Done():
			return nil, fmt.Errorf("interrupted")
		case <-retryReqInterval.C:
			// Spawn new thread for request
			pid, err := s.sentinel.RandomPeer(req.Topic)
			if err != nil {
				// Wait a bit to not exhaust CPU and skip.
				continue
			}
			log.Debug("Sent request", "pid", pid)
			s.sentinel.Peers().PeerDoRequest(pid)
			go func() {
				data, isError, err := communication.SendRequestRawToPeer(s.ctx, s.sentinel.Host(), req.Data, req.Topic, pid)
				s.sentinel.Peers().PeerFinishRequest(pid)
				if err != nil {
					s.sentinel.Peers().Penalize(pid)
					return
				} else if isError {
					s.sentinel.Peers().DisconnectPeer(pid)
				}
				select {
				case doneCh <- &sentinelrpc.ResponseData{
					Data:  data,
					Error: isError,
				}:
				default:
				}
			}()
		case resp := <-doneCh:
			return resp, nil
		}
	}
	/*
		&sentinelrpc.ResponseData{
					Data:  respData,
					Error: foundErrReq,
				}*/

}

func (s *SentinelServer) SetStatus(_ context.Context, req *sentinelrpc.Status) (*sentinelrpc.EmptyMessage, error) {
	// Send the request and get the data if we get an answer.
	s.sentinel.SetStatus(&cltypes.Status{
		ForkDigest:     utils.Uint32ToBytes4(req.ForkDigest),
		FinalizedRoot:  gointerfaces.ConvertH256ToHash(req.FinalizedRoot),
		HeadRoot:       gointerfaces.ConvertH256ToHash(req.HeadRoot),
		FinalizedEpoch: req.FinalizedEpoch,
		HeadSlot:       req.HeadSlot,
	})
	return &sentinelrpc.EmptyMessage{}, nil
}

func (s *SentinelServer) GetPeers(_ context.Context, _ *sentinelrpc.EmptyMessage) (*sentinelrpc.PeerCount, error) {
	// Send the request and get the data if we get an answer.
	return &sentinelrpc.PeerCount{
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
	log.Trace("[Sentinel Gossip] Received Packet", "topic", pkt.Topic)
	err := pkt.Codec.WritePacket(context.TODO(), pkt.Packet)
	if err != nil {
		log.Warn("[Sentinel Gossip] Error Forwarding Packet", "err", err)
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
		s.gossipNotifier.notify(sentinelrpc.GossipType_BeaconBlockGossipType, data)
	case *cltypes.SignedAggregateAndProof:
		s.gossipNotifier.notify(sentinelrpc.GossipType_AggregateAndProofGossipType, data)
	case *cltypes.SignedVoluntaryExit:
		s.gossipNotifier.notify(sentinelrpc.GossipType_VoluntaryExitGossipType, data)
	case *cltypes.ProposerSlashing:
		s.gossipNotifier.notify(sentinelrpc.GossipType_ProposerSlashingGossipType, data)
	case *cltypes.AttesterSlashing:
		s.gossipNotifier.notify(sentinelrpc.GossipType_AttesterSlashingGossipType, data)
	case *cltypes.LightClientFinalityUpdate:
		s.gossipNotifier.notify(sentinelrpc.GossipType_LightClientFinalityUpdateGossipType, data)
	case *cltypes.LightClientOptimisticUpdate:
		s.gossipNotifier.notify(sentinelrpc.GossipType_LightClientOptimisticUpdateGossipType, data)
	default:
	}
	return nil
}
