// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon-lib/gointerfaces"
	sentinelrpc "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/diagnostics/diaglib"
)

const gracePeerCount = 8

var _ sentinelrpc.SentinelServer = (*SentinelServer)(nil)

type SentinelServer struct {
	sentinelrpc.UnimplementedSentinelServer

	ctx            context.Context
	sentinel       *sentinel.Sentinel
	gossipNotifier *gossipNotifier

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

// extractSubnetIndexByGossipTopic takes a topic and extract the blob sidecar
func extractSubnetIndexByGossipTopic(name string) int {
	// e.g blob_sidecar_3, we want to extract 3
	// reject if last character is not a number
	if !unicode.IsNumber(rune(name[len(name)-1])) {
		return -1
	}
	// get the last part of the topic
	parts := strings.Split(name, "_")
	// convert it to int
	index, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		log.Warn("[Sentinel] failed to parse subnet index", "topic", name, "err", err)
		return -1
	}
	return index
}

//BanPeer(context.Context, *Peer) (*EmptyMessage, error)

func (s *SentinelServer) BanPeer(_ context.Context, p *sentinelrpc.Peer) (*sentinelrpc.EmptyMessage, error) {
	active, _, _ := s.sentinel.GetPeersCount()
	if active < gracePeerCount {
		return &sentinelrpc.EmptyMessage{}, nil
	}

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

	//trackPeerStatistics(msg.GetPeer().Pid, false, msg.Name, "unknown", len(compressedData))

	var subscription *sentinel.GossipSubscription

	switch msg.Name {
	case gossip.TopicNameBeaconBlock,
		gossip.TopicNameBeaconAggregateAndProof,
		gossip.TopicNameVoluntaryExit,
		gossip.TopicNameProposerSlashing,
		gossip.TopicNameSyncCommitteeContributionAndProof,
		gossip.TopicNameAttesterSlashing,
		gossip.TopicNameBlsToExecutionChange:
		subscription = manager.GetMatchingSubscription(msg.Name)
	default:
		// check subnets
		switch {
		case gossip.IsTopicBlobSidecar(msg.Name):
			if msg.SubnetId == nil {
				return nil, errors.New("subnetId is required for blob sidecar")
			}
			subscription = manager.GetMatchingSubscription(gossip.TopicNameBlobSidecar(*msg.SubnetId))
		case gossip.IsTopicSyncCommittee(msg.Name):
			if msg.SubnetId == nil {
				return nil, errors.New("subnetId is required for sync_committee")
			}
			subscription = manager.GetMatchingSubscription(gossip.TopicNameSyncCommittee(int(*msg.SubnetId)))
		case gossip.IsTopicBeaconAttestation(msg.Name):
			if msg.SubnetId == nil {
				return nil, errors.New("subnetId is required for beacon attestation")
			}
			subscription = manager.GetMatchingSubscription(gossip.TopicNameBeaconAttestation(*msg.SubnetId))
		case gossip.IsTopicDataColumnSidecar(msg.Name):
			if msg.SubnetId == nil {
				return nil, errors.New("subnetId is required for data column sidecar")
			}
			subscription = manager.GetMatchingSubscription(gossip.TopicNameDataColumnSidecar(*msg.SubnetId))
		default:
			return &sentinelrpc.EmptyMessage{}, fmt.Errorf("unknown topic %s", msg.Name)
		}
	}
	if subscription == nil {
		return &sentinelrpc.EmptyMessage{}, fmt.Errorf("unknown topic %s", msg.Name)
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
				SubnetId: packet.subnetId,
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

	activePeers, _, _ := s.sentinel.GetPeersCount()

	shouldBanOnFail := activePeers >= int(s.sentinel.Config().MaxPeerCount)
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
		//if strings.Contains(errorMessage.Error(), "Read Code: EOF") {
		// don't ban the peer.
		//	return nil, errorMessage
		//}
		if shouldBanOnFail {
			s.sentinel.Peers().RemovePeer(pid)
			s.sentinel.Host().Peerstore().RemovePeer(pid)
			s.sentinel.Host().Network().ClosePeer(pid)
		}
		return nil, errorMessage
	}
	// we should never get an invalid response to this. our responder should always set it on non-error response
	code, err := strconv.Atoi(resp.Header.Get("REQRESP-RESPONSE-CODE"))
	if err != nil {
		// TODO: think about how to properly handle this. should we? (or should we just assume no response is success?)
		return nil, err
	}
	// known error codes, just remove the peer
	responseCode := ResponseCode(code)
	if !responseCode.Success() {
		if shouldBanOnFail {
			s.sentinel.Peers().RemovePeer(pid)
			s.sentinel.Host().Peerstore().RemovePeer(pid)
			s.sentinel.Host().Network().ClosePeer(pid)
		}
		return nil, fmt.Errorf("peer error code: %d (%s). Error message: %s", code, responseCode.String(), responseCode.ErrorMessage(resp))
	}

	// read the body from the response
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	ans := &sentinelrpc.ResponseData{
		Data:  data,
		Error: !responseCode.Success(),
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
		if strings.Contains(err.Error(), "protocols not supported") {
			s.sentinel.Peers().RemovePeer(pid)
			s.sentinel.Host().Peerstore().RemovePeer(pid)
			s.sentinel.Host().Network().ClosePeer(pid)
			s.sentinel.Peers().SetBanStatus(pid, true)
		}
		s.logger.Trace("[sentinel] peer gave us bad data", "peer", pid, "err", err, "topic", req.Topic)
		return nil, err
	}
	return resp, nil
}

func (s *SentinelServer) SendPeerRequest(ctx context.Context, reqWithPeer *sentinelrpc.RequestDataWithPeer) (*sentinelrpc.ResponseData, error) {
	pid, err := peer.Decode(reqWithPeer.Pid)
	if err != nil {
		return nil, err
	}
	req := &sentinelrpc.RequestData{
		Data:  reqWithPeer.Data,
		Topic: reqWithPeer.Topic,
	}
	resp, err := s.requestPeer(ctx, pid, req)
	if err != nil {
		if strings.Contains(err.Error(), "protocols not supported") {
			s.sentinel.Peers().RemovePeer(pid)
			s.sentinel.Host().Peerstore().RemovePeer(pid)
			s.sentinel.Host().Network().ClosePeer(pid)
			s.sentinel.Peers().SetBanStatus(pid, true)
		}
		s.logger.Trace("[sentinel] peer gave us bad data", "peer", pid, "err", err, "topic", req.Topic)
		return nil, err
	}
	return resp, nil
}

func (s *SentinelServer) Identity(ctx context.Context, in *sentinelrpc.EmptyMessage) (*sentinelrpc.IdentityResponse, error) {
	// call s.sentinel.Identity()
	pid, enr, p2pAddresses, discoveryAddresses, metadata := s.sentinel.Identity()
	return &sentinelrpc.IdentityResponse{
		Pid:                pid,
		Enr:                enr,
		P2PAddresses:       p2pAddresses,
		DiscoveryAddresses: discoveryAddresses,
		Metadata: &sentinelrpc.Metadata{
			Seq:      metadata.SeqNumber,
			Attnets:  fmt.Sprintf("%x", metadata.Attnets),
			Syncnets: fmt.Sprintf("%x", *metadata.Syncnets),
		},
	}, nil

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
	count, connected, disconnected := s.sentinel.GetPeersCount()
	// Send the request and get the data if we get an answer.
	return &sentinelrpc.PeerCount{
		Active:       uint64(count),
		Connected:    uint64(connected),
		Disconnected: uint64(disconnected),
	}, nil
}

func (s *SentinelServer) PeersInfo(ctx context.Context, r *sentinelrpc.PeersInfoRequest) (*sentinelrpc.PeersInfoResponse, error) {
	peersInfos := s.sentinel.GetPeersInfos()
	if r.Direction == nil && r.State == nil {
		return peersInfos, nil
	}
	filtered := &sentinelrpc.PeersInfoResponse{
		Peers: make([]*sentinelrpc.Peer, 0, len(peersInfos.Peers)),
	}
	for _, peer := range peersInfos.Peers {
		if r.Direction != nil && peer.Direction != *r.Direction {
			continue
		}
		if r.State != nil && peer.State != *r.State {
			continue
		}
		filtered.Peers = append(filtered.Peers, peer)
	}
	return filtered, nil
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

func (s *SentinelServer) SetSubscribeExpiry(ctx context.Context, expiryReq *sentinelrpc.RequestSubscribeExpiry) (*sentinelrpc.EmptyMessage, error) {
	var (
		topic      = expiryReq.GetTopic()
		expiryTime = time.Unix(int64(expiryReq.GetExpiryUnixSecs()), 0)
	)
	subs := s.sentinel.GossipManager().GetMatchingSubscription(topic)
	if subs == nil {
		return nil, errors.New("no such subscription")
	}
	subs.OverwriteSubscriptionExpiry(expiryTime)
	return &sentinelrpc.EmptyMessage{}, nil
}

func (s *SentinelServer) handleGossipPacket(pkt *sentinel.GossipMessage) error {
	var err error
	s.logger.Trace("[Sentinel Gossip] Received Packet", "topic", pkt.TopicName)

	data := pkt.Data
	topic := pkt.TopicName
	// If we use snappy codec then decompress it accordingly.
	if strings.Contains(topic, sentinel.SSZSnappyCodec) {
		data, err = utils.DecompressSnappy(data, true)
		if err != nil {
			return err
		}
	}
	textPid, err := pkt.From.MarshalText()
	if err != nil {
		return err
	}

	msgType, gossipTopic := parseTopic(topic)
	trackPeerStatistics(string(textPid), true, msgType, gossipTopic, len(data))

	switch gossipTopic {
	case gossip.TopicNameBeaconBlock,
		gossip.TopicNameBeaconAggregateAndProof,
		gossip.TopicNameVoluntaryExit,
		gossip.TopicNameProposerSlashing,
		gossip.TopicNameAttesterSlashing,
		gossip.TopicNameBlsToExecutionChange,
		gossip.TopicNameSyncCommitteeContributionAndProof:
		s.gossipNotifier.notify(&gossipObject{
			data:     data,
			t:        gossipTopic,
			pid:      string(textPid),
			subnetId: nil,
		})
	default:
		// case for:
		// TopicNamePrefixBlobSidecar
		// TopicNamePrefixBeaconAttestation
		// TopicNamePrefixSyncCommittee
		subnet := extractSubnetIndexByGossipTopic(gossipTopic)
		if subnet < 0 {
			break
		}
		subnetId := uint64(subnet)
		s.gossipNotifier.notify(&gossipObject{
			data:     data,
			t:        gossipTopic,
			pid:      string(textPid),
			subnetId: &subnetId,
		})
	}
	return nil
}

func trackPeerStatistics(peerID string, inbound bool, msgType string, msgCap string, bytes int) {
	isDiagEnabled := diaglib.TypeOf(diaglib.PeerStatisticMsgUpdate{}).Enabled()
	if isDiagEnabled {
		diaglib.Send(diaglib.PeerStatisticMsgUpdate{
			PeerName: "TODO",
			PeerType: "Sentinel",
			PeerID:   peerID,
			Inbound:  inbound,
			MsgType:  msgType,
			MsgCap:   msgCap,
			Bytes:    bytes,
		})
	}
}

func parseTopic(input string) (string, string) {
	// e.g /eth2/d31f6191/blob_sidecar_3/ssz_snappy
	parts := strings.Split(input, "/")

	if len(parts) < 4 {
		return "unknown", "unknown"
	}

	capability := parts[1]
	topick := parts[3]

	return capability, topick
}

type ResponseCode int

func (r ResponseCode) String() string {
	switch r {
	case 0:
		return "success"
	case 1:
		return "invalid request"
	case 2:
		return "server error"
	case 3:
		return "resource unavailable"
	}
	return "unknown"
}

func (r ResponseCode) Success() bool {
	return r == 0
}

func (r ResponseCode) ErrorMessage(resp *http.Response) string {
	if r == 0 || r == 1 {
		return ""
	}
	errBody, _ := io.ReadAll(resp.Body)
	return string(errBody)
}
