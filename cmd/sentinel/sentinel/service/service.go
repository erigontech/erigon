package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
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

//BanPeer(context.Context, *Peer) (*EmptyMessage, error)

func (s *SentinelServer) BanPeer(_ context.Context, p *sentinelrpc.Peer) (*sentinelrpc.EmptyMessage, error) {
	var pid peer.ID
	if err := pid.UnmarshalText([]byte(p.Pid)); err != nil {
		return nil, err
	}
	s.sentinel.Peers().BanBadPeer(pid)
	return &sentinelrpc.EmptyMessage{}, nil
}

func (s *SentinelServer) PublishGossip(_ context.Context, msg *sentinelrpc.GossipData) (*sentinelrpc.EmptyMessage, error) {
	manager := s.sentinel.GossipManager()
	// Snappify payload before sending it to gossip
	compressedData := utils.CompressSnappy(msg.Data)
	var subscription *sentinel.GossipSubscription

	switch msg.Type {
	case sentinelrpc.GossipType_BeaconBlockGossipType:
		subscription = manager.GetMatchingSubscription(string(sentinel.BeaconBlockTopic))
	case sentinelrpc.GossipType_AggregateAndProofGossipType:
		subscription = manager.GetMatchingSubscription(string(sentinel.BeaconAggregateAndProofTopic))
	case sentinelrpc.GossipType_VoluntaryExitGossipType:
		subscription = manager.GetMatchingSubscription(string(sentinel.VoluntaryExitTopic))
	case sentinelrpc.GossipType_ProposerSlashingGossipType:
		subscription = manager.GetMatchingSubscription(string(sentinel.ProposerSlashingTopic))
	case sentinelrpc.GossipType_AttesterSlashingGossipType:
		subscription = manager.GetMatchingSubscription(string(sentinel.AttesterSlashingTopic))
	default:
		return &sentinelrpc.EmptyMessage{}, nil
	}
	if subscription == nil {
		return &sentinelrpc.EmptyMessage{}, nil
	}
	return &sentinelrpc.EmptyMessage{}, subscription.Publish(compressedData)
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
				Peer: &sentinelrpc.Peer{
					Pid: packet.pid,
				},
			}); err != nil {
				log.Warn("[Sentinel] Could not relay gossip packet", "reason", err)
			}
		}
	}
}

func (s *SentinelServer) SendRequest(_ context.Context, req *sentinelrpc.RequestData) (*sentinelrpc.ResponseData, error) {
	retryReqInterval := time.NewTicker(20 * time.Millisecond)
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
			//log.Trace("[sentinel] Sent request", "pid", pid)
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

func (s *SentinelServer) handleGossipPacket(pkt *pubsub.Message) error {
	var err error
	log.Trace("[Sentinel Gossip] Received Packet", "topic", pkt.Topic)
	data := pkt.GetData()

	// If we use snappy codec then decompress it accordingly.
	if strings.Contains(*pkt.Topic, sentinel.SSZSnappyCodec) {
		data, err = utils.DecompressSnappy(data)
		if err != nil {
			return err
		}
	}
	textPid, err := pkt.ReceivedFrom.MarshalText()
	if err != nil {
		return err
	}
	// Check to which gossip it belongs to.
	if strings.Contains(*pkt.Topic, string(sentinel.BeaconBlockTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_BeaconBlockGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.BeaconAggregateAndProofTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_AggregateAndProofGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.VoluntaryExitTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_VoluntaryExitGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.ProposerSlashingTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_ProposerSlashingGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.AttesterSlashingTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_AttesterSlashingGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.LightClientFinalityUpdateTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_LightClientFinalityUpdateGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.LightClientOptimisticUpdateTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_LightClientOptimisticUpdateGossipType, data, string(textPid))
	}
	return nil
}
