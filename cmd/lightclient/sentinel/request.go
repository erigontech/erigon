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
	"io"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/handlers"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// TODO: add the rest of the request topics
func (s *Sentinel) SendRequestRaw(data []byte, topic string) ([]byte, bool, error) {
	var (
		peerInfo *peer.AddrInfo
		err      error
	)
	if strings.Contains(topic, "light_client") {
		peerInfo, err = connectToRandomLightClientPeer(s)
	} else {
		_, peerInfo, err = connectToRandomPeer(s)
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to a random peer err=%s", err)
	}

	peerId := peerInfo.ID

	reqRetryTimer := time.NewTimer(clparams.ReqTimeout)
	defer reqRetryTimer.Stop()

	retryTicker := time.NewTicker(10 * time.Millisecond)
	defer retryTicker.Stop()

	stream, err := writeRequestRaw(s, data, peerId, topic)
	for err != nil {
		select {
		case <-s.ctx.Done():
			log.Warn("[Req] sentinel has been shut down")
			return nil, false, nil
		case <-reqRetryTimer.C:
			log.Debug("[Req] timeout", "topic", topic, "peer", peerId)
			return nil, false, err
		case <-retryTicker.C:
			stream, err = writeRequestRaw(s, data, peerId, topic)
		}
	}

	defer stream.Close()
	log.Debug("[Req] sent request", "topic", topic, "peer", peerId)

	respRetryTimer := time.NewTimer(clparams.RespTimeout)
	defer respRetryTimer.Stop()

	resp, foundErrRequest, err := verifyResponse(stream, peerId)
	for err != nil {
		select {
		case <-s.ctx.Done():
			log.Warn("[Resp] sentinel has been shutdown")
			return nil, false, nil
		case <-respRetryTimer.C:
			log.Debug("[Resp] timeout", "topic", topic, "peer", peerId)
			return nil, false, err
		case <-retryTicker.C:
			resp, foundErrRequest, err = verifyResponse(stream, peerId)
		}
	}

	return resp, foundErrRequest, nil
}

func writeRequestRaw(s *Sentinel, data []byte, peerId peer.ID, topic string) (network.Stream, error) {
	stream, err := s.host.NewStream(s.ctx, peerId, protocol.ID(topic))
	if err != nil {
		return nil, fmt.Errorf("failed to begin stream, err=%s", err)
	}

	if _, ok := handlers.NoRequestHandlers[topic]; !ok {
		if _, err := stream.Write(data); err != nil {
			return nil, err
		}
	}

	return stream, stream.CloseWrite()
}

func verifyResponse(stream network.Stream, peerId peer.ID) ([]byte, bool, error) {
	code := make([]byte, 1)
	_, err := stream.Read(code)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read code byte peer=%s, err=%s", peerId, err)
	}

	message, err := io.ReadAll(stream)
	if err != nil {
		return nil, false, err
	}

	return common.CopyBytes(message), code[0] != 0, nil
}
