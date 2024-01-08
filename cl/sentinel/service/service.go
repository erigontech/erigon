package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/sentinel"
	"github.com/ledgerwatch/erigon/cl/sentinel/httpreqresp"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

var _ sentinelrpc.SentinelServer = (*SentinelServer)(nil)

type SentinelServer struct {
	sentinelrpc.UnimplementedSentinelServer

	ctx            context.Context
	sentinel       *sentinel.Sentinel
	gossipNotifier *gossipNotifier

	mu     sync.RWMutex
	logger log.Logger

	peerStatistics map[string]*diagnostics.PeerStatistics
}

func NewSentinelServer(ctx context.Context, sentinel *sentinel.Sentinel, logger log.Logger) *SentinelServer {
	return &SentinelServer{
		sentinel:       sentinel,
		ctx:            ctx,
		gossipNotifier: newGossipNotifier(),
		logger:         logger,
		peerStatistics: make(map[string]*diagnostics.PeerStatistics),
	}
}

// extractBlobSideCarIndex takes a topic and extract the blob sidecar
func extractBlobSideCarIndex(topic string) int {
	// compute the index prefixless
	startIndex := strings.Index(topic, gossip.TopicNamePrefixBlobSidecar) + len(gossip.TopicNamePrefixBlobSidecar)
	endIndex := strings.Index(topic[:startIndex], "/")
	blobIndex, err := strconv.Atoi(topic[startIndex:endIndex])
	if err != nil {
		panic(fmt.Sprintf("should not be substribed to %s", topic))
	}
	return blobIndex
}

//BanPeer(context.Context, *Peer) (*EmptyMessage, error)

func (s *SentinelServer) BanPeer(_ context.Context, p *sentinelrpc.Peer) (*sentinelrpc.EmptyMessage, error) {
	var pid peer.ID
	if err := pid.UnmarshalText([]byte(p.Pid)); err != nil {
		return nil, err
	}
	s.sentinel.Peers().SetBanStatus(pid, true)
	s.sentinel.Host().Peerstore().RemovePeer(pid)
	s.sentinel.Host().Network().ClosePeer(pid)
	return &sentinelrpc.EmptyMessage{}, nil
}

func (s *SentinelServer) PublishGossip(_ context.Context, msg *sentinelrpc.GossipData) (*sentinelrpc.EmptyMessage, error) {
	manager := s.sentinel.GossipManager()
	// Snappify payload before sending it to gossip
	compressedData := utils.CompressSnappy(msg.Data)

	s.trackPeerStatistics(msg.GetPeer().Pid, false, msg.Name, "unknown", len(compressedData))

	var subscription *sentinel.GossipSubscription

	// TODO: this is still wrong... we should build a subscription here to match exactly, meaning that downstream consumers should be
	// in charge of keeping track of fork id.
	switch msg.Name {
	case gossip.TopicNameBeaconBlock:
		subscription = manager.GetMatchingSubscription(msg.Name)
	case gossip.TopicNameBeaconAggregateAndProof:
		subscription = manager.GetMatchingSubscription(msg.Name)
	case gossip.TopicNameVoluntaryExit:
		subscription = manager.GetMatchingSubscription(msg.Name)
	case gossip.TopicNameProposerSlashing:
		subscription = manager.GetMatchingSubscription(msg.Name)
	case gossip.TopicNameAttesterSlashing:
		subscription = manager.GetMatchingSubscription(msg.Name)
	default:
		switch {
		case gossip.IsTopicBlobSidecar(msg.Name):
			subscription = manager.GetMatchingSubscription(msg.Name)
		default:
			return &sentinelrpc.EmptyMessage{}, nil
		}
	}
	if subscription == nil {
		return &sentinelrpc.EmptyMessage{}, nil
	}
	return &sentinelrpc.EmptyMessage{}, subscription.Publish(compressedData)
}

func (s *SentinelServer) SubscribeGossip(data *sentinelrpc.SubscriptionData, stream sentinelrpc.Sentinel_SubscribeGossipServer) error {
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
			if !s.gossipMatchSubscription(packet, data) {
				continue
			}
			if err := stream.Send(&sentinelrpc.GossipData{
				Data: packet.data,
				Name: packet.t,
				Peer: &sentinelrpc.Peer{
					Pid: packet.pid,
				},
			}); err != nil {
				s.logger.Warn("[Sentinel] Could not relay gossip packet", "reason", err)
			}
		}
	}
}

func (s *SentinelServer) gossipMatchSubscription(obj gossipObject, data *sentinelrpc.SubscriptionData) bool {
	if data.Filter != nil {
		filter := data.GetFilter()
		matched, err := path.Match(obj.t, filter)
		if err != nil || !matched {
			return false
		}
	}
	return true
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

func (s *SentinelServer) requestPeer(ctx context.Context, pid peer.ID, req *sentinelrpc.RequestData) (*sentinelrpc.ResponseData, error) {
	// prepare the http request
	httpReq, err := http.NewRequest("GET", "http://service.internal/", bytes.NewBuffer(req.Data))
	if err != nil {
		return nil, err
	}
	// set the peer and topic we are requesting
	httpReq.Header.Set("REQRESP-PEER-ID", pid.String())
	httpReq.Header.Set("REQRESP-TOPIC", req.Topic)
	// for now this can't actually error. in the future, it can due to a network error
	resp, err := httpreqresp.Do(s.sentinel.ReqRespHandler(), httpReq)
	if err != nil {
		// we remove, but dont ban the peer if we fail. this is because its probably not their fault, but maybe it is.
		return nil, err
	}
	defer resp.Body.Close()
	// some standard http error code parsing
	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		errBody, _ := io.ReadAll(resp.Body)
		errorMessage := fmt.Errorf("SentinelHttp: %s", string(errBody))
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			s.sentinel.Peers().RemovePeer(pid)
			s.sentinel.Host().Peerstore().RemovePeer(pid)
			s.sentinel.Host().Network().ClosePeer(pid)
		}
		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			s.sentinel.Host().Peerstore().RemovePeer(pid)
			s.sentinel.Host().Network().ClosePeer(pid)
		}
		return nil, errorMessage
	}
	// we should never get an invalid response to this. our responder should always set it on non-error response
	isError, err := strconv.Atoi(resp.Header.Get("REQRESP-RESPONSE-CODE"))
	if err != nil {
		// TODO: think about how to properly handle this. should we? (or should we just assume no response is success?)
		return nil, err
	}
	// known error codes, just remove the peer
	if isError == 3 || isError == 2 {
		s.sentinel.Host().Peerstore().RemovePeer(pid)
		s.sentinel.Host().Network().ClosePeer(pid)
		return nil, fmt.Errorf("peer error code: %d", isError)
	}
	// unknown error codes
	if isError > 3 {
		s.logger.Debug("peer returned unknown erro", "id", pid.String())
		s.sentinel.Host().Peerstore().RemovePeer(pid)
		s.sentinel.Host().Network().ClosePeer(pid)
		return nil, fmt.Errorf("peer returned unknown error: %d", isError)
	}
	// read the body from the response
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	ans := &sentinelrpc.ResponseData{
		Data:  data,
		Error: isError != 0,
		Peer: &sentinelrpc.Peer{
			Pid: pid.String(),
		},
	}
	return ans, nil

}

func (s *SentinelServer) SendRequest(ctx context.Context, req *sentinelrpc.RequestData) (*sentinelrpc.ResponseData, error) {
	// Try finding the data to our peers
	// this is using return statements instead of continue, since it saves a few lines
	// but me writing this comment has put them back.. oh no!!! anyways, returning true means we stop.
	peer, done, err := s.sentinel.Peers().Request()
	if err != nil {
		return nil, err
	}
	defer done()
	pid := peer.Id()

	resp, err := s.requestPeer(ctx, pid, req)
	if err != nil {
		s.logger.Trace("[sentinel] peer gave us bad data", "peer", pid, "err", err)
		return nil, err
	}
	return resp, nil

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

func (s *SentinelServer) handleGossipPacket(pkt *sentinel.GossipMessage) error {
	var err error
	s.logger.Trace("[Sentinel Gossip] Received Packet", "topic", pkt.TopicName)

	data := pkt.Data
	topic := pkt.TopicName
	// If we use snappy codec then decompress it accordingly.
	if strings.Contains(topic, sentinel.SSZSnappyCodec) {
		data, err = utils.DecompressSnappy(data)
		if err != nil {
			return err
		}
	}
	textPid, err := pkt.From.MarshalText()
	if err != nil {
		return err
	}

	msgType, msgCap := parseTopic(topic)
	s.trackPeerStatistics(string(textPid), true, msgType, msgCap, len(data))

	// Check to which gossip it belongs to.
	if strings.Contains(topic, string(gossip.TopicNameBeaconBlock)) {
		s.gossipNotifier.notify(gossip.TopicNameBeaconBlock, data, string(textPid))
	} else if strings.Contains(topic, string(gossip.TopicNameBeaconAggregateAndProof)) {
		s.gossipNotifier.notify(gossip.TopicNameBeaconAggregateAndProof, data, string(textPid))
	} else if strings.Contains(topic, string(gossip.TopicNameVoluntaryExit)) {
		s.gossipNotifier.notify(gossip.TopicNameVoluntaryExit, data, string(textPid))
	} else if strings.Contains(topic, string(gossip.TopicNameProposerSlashing)) {
		s.gossipNotifier.notify(gossip.TopicNameProposerSlashing, data, string(textPid))
	} else if strings.Contains(topic, string(gossip.TopicNameAttesterSlashing)) {
		s.gossipNotifier.notify(gossip.TopicNameAttesterSlashing, data, string(textPid))
	} else if strings.Contains(topic, string(gossip.TopicNameBlsToExecutionChange)) {
		s.gossipNotifier.notify(gossip.TopicNameBlsToExecutionChange, data, string(textPid))
	} else if gossip.IsTopicBlobSidecar(topic) {
		// extract the index
		s.gossipNotifier.notifyBlob(data, string(textPid), extractBlobSideCarIndex(topic))
	}
	return nil
}

func (s *SentinelServer) GetPeersStatistics() map[string]*diagnostics.PeerStatistics {
	stats := make(map[string]*diagnostics.PeerStatistics)
	for k, v := range s.peerStatistics {
		stats[k] = v
		delete(s.peerStatistics, k)
	}

	return stats
}

func (s *SentinelServer) trackPeerStatistics(peerID string, inbound bool, msgType string, msgCap string, bytes int) {
	if s.peerStatistics == nil {
		s.peerStatistics = make(map[string]*diagnostics.PeerStatistics)
	}

	if _, exists := s.peerStatistics[peerID]; !exists {
		s.peerStatistics[peerID] = &diagnostics.PeerStatistics{
			CapBytesIn:   make(map[string]uint64),
			CapBytesOut:  make(map[string]uint64),
			TypeBytesIn:  make(map[string]uint64),
			TypeBytesOut: make(map[string]uint64),
		}
	}

	stats := s.peerStatistics[peerID]

	if inbound {
		stats.BytesIn += uint64(bytes)
		stats.CapBytesIn[msgCap] += uint64(bytes)
		stats.TypeBytesIn[msgType] += uint64(bytes)
	} else {
		stats.BytesOut += uint64(bytes)
		stats.CapBytesOut[msgCap] += uint64(bytes)
		stats.TypeBytesOut[msgType] += uint64(bytes)
	}
}

func parseTopic(input string) (string, string) {
	parts := strings.Split(input, "/")

	if len(parts) < 4 {
		return "unknown", "unknown"
	}

	capability := parts[1]
	topick := parts[3]

	return capability, topick
}
