/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/handlers"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func (s *Sentinel) SendPingReqV1() (communication.Packet, error) {
	requestPacket := &p2p.Ping{
		Id: s.metadataV1.SeqNumber,
	}
	responsePacket := &p2p.Ping{}
	return sendRequest(s, requestPacket, responsePacket, handlers.PingProtocolV1)
}

func (s *Sentinel) SendMetadataReqV1() (communication.Packet, error) {
	requestPacket := &lightrpc.MetadataV1{}
	responsePacket := &lightrpc.MetadataV1{}

	return sendRequest(s, requestPacket, responsePacket, handlers.MetadataProtocolV1)
}

// TODO: add the rest of the request topics

func sendRequest(s *Sentinel, requestPacket communication.Packet, responsePacket communication.Packet, topic string) (communication.Packet, error) {
	_, peerInfo, err := connectToRandomPeer(s)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to a random peer err=%s", err)
	}

	peerId := peerInfo.ID

	reqRetryTimer := time.NewTimer(clparams.ReqTimeout)
	defer reqRetryTimer.Stop()

	retryTicker := time.NewTicker(10 * time.Millisecond)
	defer retryTicker.Stop()

	sc, err := writeRequest(s, requestPacket, peerId, topic)
	for err != nil {
		select {
		case <-s.ctx.Done():
			log.Warn("[Req] sentinel has been shut down")
			return nil, nil
		case <-reqRetryTimer.C:
			log.Debug("[Req] timeout", "topic", topic, "peer", peerId)
			return nil, err
		case <-retryTicker.C:
			sc, err = writeRequest(s, requestPacket, peerId, topic)
		}
	}

	defer sc.Close()
	log.Debug("[Req] sent request", "topic", topic, "peer", peerId)

	respRetryTimer := time.NewTimer(clparams.RespTimeout)
	defer respRetryTimer.Stop()

	responsePacket, err = decodeResponse(sc, responsePacket, peerId)
	for err != nil {
		select {
		case <-s.ctx.Done():
			log.Warn("[Resp] sentinel has been shutdown")
			return nil, nil
		case <-respRetryTimer.C:
			log.Debug("[Resp] timeout", "topic", topic, "peer", peerId)
			return nil, err
		case <-retryTicker.C:
			responsePacket, err = decodeResponse(sc, responsePacket, peerId)
		}
	}

	return responsePacket, nil
}

func writeRequest(s *Sentinel, requestPacket communication.Packet, peerId peer.ID, topic string) (communication.StreamCodec, error) {
	stream, err := s.host.NewStream(s.ctx, peerId, protocol.ID(topic))
	if err != nil {
		return nil, fmt.Errorf("failed to begin stream, err=%s", err)
	}

	sc := ssz_snappy.NewStreamCodec(stream)

	if _, err := sc.WritePacket(requestPacket); err != nil {
		return nil, fmt.Errorf("failed to write packet type=%s, err=%s", reflect.TypeOf(requestPacket), err)
	}

	if err := sc.CloseWriter(); err != nil {
		return nil, fmt.Errorf("failed to close write stream, err=%s", err)
	}

	return sc, nil
}

func decodeResponse(sc communication.StreamCodec, responsePacket communication.Packet, peerId peer.ID) (communication.Packet, error) {
	code, err := sc.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read code byte peer=%s, err=%s", peerId, err)
	}

	if code != 0 {
		errPacket := &communication.ErrorMessage{}
		protoCtx, err := sc.Decode(errPacket)
		if err != nil {
			return nil, fmt.Errorf("failed to decode error packet got=%s, err=%s", string(protoCtx.Raw), err)
		}
		log.Debug("[Resp] got error packet", "error-message", string(errPacket.Message), "peer", peerId)
		return errPacket, nil
	}

	protoCtx, err := sc.Decode(responsePacket)
	if err != nil {
		return nil, fmt.Errorf("failed to decode packet got=%s, err=%s", string(protoCtx.Raw), err)
	}
	log.Debug("[Resp] got response from", "response", responsePacket, "peer", peerId)

	return responsePacket, nil
}
