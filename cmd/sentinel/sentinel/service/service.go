package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/peers"
	"github.com/ledgerwatch/log/v3"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type SentinelServer struct {
	sentinelrpc.UnimplementedSentinelServer

	ctx            context.Context
	sentinel       *sentinel.Sentinel
	gossipNotifier *gossipNotifier

	mu     sync.RWMutex
	logger log.Logger
}

func NewSentinelServer(ctx context.Context, sentinel *sentinel.Sentinel, logger log.Logger) *SentinelServer {
	return &SentinelServer{
		sentinel:       sentinel,
		ctx:            ctx,
		gossipNotifier: newGossipNotifier(),
		logger:         logger,
	}
}

// extractBlobSideCarIndex takes a topic and extract the blob sidecar
func extractBlobSideCarIndex(topic string) int {
	// compute the index prefixless
	startIndex := strings.Index(topic, string(sentinel.BlobSidecarTopic)) + len(sentinel.BlobSidecarTopic)
	endIndex := strings.Index(topic[:startIndex], "/")
	blobIndex, err := strconv.Atoi(topic[startIndex:endIndex])
	if err != nil {
		panic(fmt.Sprintf("should not be substribed to %s", topic))
	}
	return blobIndex
}

//BanPeer(context.Context, *Peer) (*EmptyMessage, error)

func (s *SentinelServer) BanPeer(_ context.Context, p *sentinelrpc.Peer) (*sentinelrpc.EmptyMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var pid peer.ID
	if err := pid.UnmarshalText([]byte(p.Pid)); err != nil {
		return nil, err
	}
	s.sentinel.Peers().WithPeer(pid, func(peer *peers.Peer) {
		peer.Ban()
	})
	return &sentinelrpc.EmptyMessage{}, nil
}

func (s *SentinelServer) PublishGossip(_ context.Context, msg *sentinelrpc.GossipData) (*sentinelrpc.EmptyMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	case sentinelrpc.GossipType_BlobSidecarType:
		if msg.BlobIndex == nil {
			return &sentinelrpc.EmptyMessage{}, errors.New("cannot publish sidecar blob with no index")
		}
		subscription = manager.GetMatchingSubscription(fmt.Sprintf(string(sentinel.BlobSidecarTopic), *msg.BlobIndex))
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
				BlobIndex: packet.blobIndex,
			}); err != nil {
				s.logger.Warn("[Sentinel] Could not relay gossip packet", "reason", err)
			}
		}
	}
}

func (s *SentinelServer) withTimeoutCtx(pctx context.Context, dur time.Duration) (ctx context.Context, cn func()) {
	if dur > 0 {
		ctx, cn = context.WithTimeout(pctx, 8*time.Second)
	} else {
		ctx, cn = context.WithCancel(pctx)
	}
	go func() {
		select {
		case <-s.ctx.Done():
			cn()
		case <-ctx.Done():
			return
		}
	}()
	return ctx, cn
}

func (s *SentinelServer) SendRequest(ctx context.Context, req *sentinelrpc.RequestData) (*sentinelrpc.ResponseData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	retryReqInterval := time.NewTicker(100 * time.Millisecond)
	defer retryReqInterval.Stop()
	doneCh := make(chan *sentinelrpc.ResponseData)
	// Try finding the data to our peers
	uniquePeers := map[peer.ID]struct{}{}
	requestPeer := func(peer *peers.Peer) {
		peer.MarkUsed()
		defer peer.MarkUnused()
		data, isError, err := communication.SendRequestRawToPeer(ctx, s.sentinel.Host(), req.Data, req.Topic, peer.ID())
		if err != nil {
			if strings.Contains(err.Error(), "protocols not supported") {
				peer.Ban("peer does not support protocol")
			}
			return
		}
		if isError > 3 {
			peer.Disconnect(fmt.Sprintf("invalid response, starting byte %d", isError))
		}
		if isError != 0 {
			peer.Penalize()
			return
		}
		ans := &sentinelrpc.ResponseData{
			Data:  data,
			Error: isError != 0,
			Peer: &sentinelrpc.Peer{
				Pid: peer.ID().String(),
			},
		}
		select {
		case doneCh <- ans:
			peer.MarkReplied()
			retryReqInterval.Stop()
			return
		case <-ctx.Done():
			return
		}
	}
	go func() {
		for {
			pid, err := s.sentinel.RandomPeer(req.Topic)
			if err != nil {
				continue
			}
			if _, ok := uniquePeers[pid]; !ok {
				go s.sentinel.Peers().WithPeer(pid, requestPeer)
				uniquePeers[pid] = struct{}{}
			}
			select {
			case <-retryReqInterval.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	select {
	case resp := <-doneCh:
		return resp, nil
	case <-ctx.Done():
		return &sentinelrpc.ResponseData{
			Data:  []byte("request timeout"),
			Error: true,
			Peer:  &sentinelrpc.Peer{Pid: ""},
		}, nil
	}
}

func (s *SentinelServer) SetStatus(_ context.Context, req *sentinelrpc.Status) (*sentinelrpc.EmptyMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Send the request and get the data if we get an answer.
	return &sentinelrpc.PeerCount{
		Amount: uint64(s.sentinel.GetPeersCount()),
	}, nil
}

func (s *SentinelServer) ListenToGossip() {
	refreshTicker := time.NewTicker(100 * time.Millisecond)
	defer refreshTicker.Stop()
	for {
		s.mu.RLock()
		select {
		case pkt := <-s.sentinel.RecvGossip():
			s.handleGossipPacket(pkt)
		case <-s.ctx.Done():
			return
		case <-refreshTicker.C:
		}
		s.mu.RUnlock()
	}
}

func (s *SentinelServer) handleGossipPacket(pkt *pubsub.Message) error {
	var err error
	s.logger.Trace("[Sentinel Gossip] Received Packet", "topic", pkt.Topic)
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
	} else if strings.Contains(*pkt.Topic, string(sentinel.BlsToExecutionChangeTopic)) {
		s.gossipNotifier.notify(sentinelrpc.GossipType_BlsToExecutionChangeGossipType, data, string(textPid))
	} else if strings.Contains(*pkt.Topic, string(sentinel.BlobSidecarTopic)) {
		// extract the index
		s.gossipNotifier.notifyBlob(sentinelrpc.GossipType_BlobSidecarType, data, string(textPid), extractBlobSideCarIndex(*pkt.Topic))
	}
	return nil
}
