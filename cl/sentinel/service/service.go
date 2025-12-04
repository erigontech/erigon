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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

const gracePeerCount = 32

var _ sentinelproto.SentinelServer = (*SentinelServer)(nil)

type SentinelServer struct {
	sentinelproto.UnimplementedSentinelServer

	ctx      context.Context
	sentinel *sentinel.Sentinel

	logger log.Logger
}

func NewSentinelServer(ctx context.Context, sentinel *sentinel.Sentinel, logger log.Logger) *SentinelServer {
	return &SentinelServer{
		sentinel: sentinel,
		ctx:      ctx,
		logger:   logger,
	}
}

func (s *SentinelServer) BanPeer(_ context.Context, p *sentinelproto.Peer) (*sentinelproto.EmptyMessage, error) {
	active, _, _ := s.sentinel.GetPeersCount()
	if active < gracePeerCount {
		return &sentinelproto.EmptyMessage{}, nil
	}

	var pid peer.ID
	if err := pid.UnmarshalText([]byte(p.Pid)); err != nil {
		return nil, err
	}
	s.sentinel.Peers().SetBanStatus(pid, true)
	s.sentinel.Host().Peerstore().RemovePeer(pid)
	s.sentinel.Host().Network().ClosePeer(pid)
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *SentinelServer) PublishGossip(_ context.Context, msg *sentinelproto.GossipData) (*sentinelproto.EmptyMessage, error) {
	panic("do not call this")
}

func (s *SentinelServer) SubscribeGossip(data *sentinelproto.SubscriptionData, stream sentinelproto.Sentinel_SubscribeGossipServer) error {
	panic("do not call this")
}

func (s *SentinelServer) requestPeer(ctx context.Context, pid peer.ID, req *sentinelproto.RequestData) (*sentinelproto.ResponseData, error) {
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
	ans := &sentinelproto.ResponseData{
		Data:  data,
		Error: !responseCode.Success(),
		Peer: &sentinelproto.Peer{
			Pid: pid.String(),
		},
	}
	return ans, nil

}

func (s *SentinelServer) SendRequest(ctx context.Context, req *sentinelproto.RequestData) (*sentinelproto.ResponseData, error) {
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

func (s *SentinelServer) SendPeerRequest(ctx context.Context, reqWithPeer *sentinelproto.RequestDataWithPeer) (*sentinelproto.ResponseData, error) {
	pid, err := peer.Decode(reqWithPeer.Pid)
	if err != nil {
		return nil, err
	}
	req := &sentinelproto.RequestData{
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

func (s *SentinelServer) Identity(ctx context.Context, in *sentinelproto.EmptyMessage) (*sentinelproto.IdentityResponse, error) {
	// call s.sentinel.Identity()
	pid, enr, p2pAddresses, discoveryAddresses, metadata := s.sentinel.Identity()
	return &sentinelproto.IdentityResponse{
		Pid:                pid,
		Enr:                enr,
		P2PAddresses:       p2pAddresses,
		DiscoveryAddresses: discoveryAddresses,
		Metadata: &sentinelproto.Metadata{
			Seq:      metadata.SeqNumber,
			Attnets:  fmt.Sprintf("%x", metadata.Attnets),
			Syncnets: fmt.Sprintf("%x", *metadata.Syncnets),
		},
	}, nil

}

func (s *SentinelServer) SetStatus(_ context.Context, req *sentinelproto.Status) (*sentinelproto.EmptyMessage, error) {
	// Send the request and get the data if we get an answer.
	s.sentinel.SetStatus(&cltypes.Status{
		ForkDigest:     utils.Uint32ToBytes4(req.ForkDigest),
		FinalizedRoot:  gointerfaces.ConvertH256ToHash(req.FinalizedRoot),
		HeadRoot:       gointerfaces.ConvertH256ToHash(req.HeadRoot),
		FinalizedEpoch: req.FinalizedEpoch,
		HeadSlot:       req.HeadSlot,
	})
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *SentinelServer) GetPeers(_ context.Context, _ *sentinelproto.EmptyMessage) (*sentinelproto.PeerCount, error) {
	count, connected, disconnected := s.sentinel.GetPeersCount()
	// Send the request and get the data if we get an answer.
	return &sentinelproto.PeerCount{
		Active:       uint64(count),
		Connected:    uint64(connected),
		Disconnected: uint64(disconnected),
	}, nil
}

func (s *SentinelServer) PeersInfo(ctx context.Context, r *sentinelproto.PeersInfoRequest) (*sentinelproto.PeersInfoResponse, error) {
	peersInfos := s.sentinel.GetPeersInfos()
	if r.Direction == nil && r.State == nil {
		return peersInfos, nil
	}
	filtered := &sentinelproto.PeersInfoResponse{
		Peers: make([]*sentinelproto.Peer, 0, len(peersInfos.Peers)),
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

func (s *SentinelServer) SetSubscribeExpiry(ctx context.Context, expiryReq *sentinelproto.RequestSubscribeExpiry) (*sentinelproto.EmptyMessage, error) {
	panic("do not call this")
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
