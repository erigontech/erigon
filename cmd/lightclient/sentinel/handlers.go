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
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	ssz "github.com/prysmaticlabs/fastssz"
)

var (
	ProtocolPrefix = "/eth2/beacon_chain/req"
	reservedBytes  = 128
)

var handlers map[protocol.ID]network.StreamHandler = map[protocol.ID]network.StreamHandler{
	protocol.ID(ProtocolPrefix + "/ping/1/ssz_snappy"):                   pingHandler,
	protocol.ID(ProtocolPrefix + "/status/1/ssz_snappy"):                 statusHandler,
	protocol.ID(ProtocolPrefix + "/goodbye/1/ssz_snappy"):                goodbyeHandler,
	protocol.ID(ProtocolPrefix + "/beacon_blocks_by_range/1/ssz_snappy"): blocksByRangeHandler,
	protocol.ID(ProtocolPrefix + "/beacon_blocks_by_root/1/ssz_snappy"):  beaconBlocksByRootHandler,
	protocol.ID(ProtocolPrefix + "/metadata/1/ssz_snappy"):               metadataHandler,
}

func setDeadLines(stream network.Stream) {
	if err := stream.SetReadDeadline(time.Now().Add(clparams.TtfbTimeout)); err != nil {
		log.Error("failed to set stream read dead line", "err", err)
	}
	if err := stream.SetWriteDeadline(time.Now().Add(clparams.RespTimeout)); err != nil {
		log.Error("failed to set stream write dead line", "err", err)
	}
}

func pingHandler(stream network.Stream) {
	pingBytes := make([]byte, 8)
	setDeadLines(stream)
	n, err := stream.Read(pingBytes)
	if err != nil {
		log.Warn("handler crashed", "err", err)
		return
	}
	if n != 8 {
		log.Warn("Invalid ping received")
		return
	}
	log.Info("[Lightclient] Received", "ping", ssz.UnmarshallUint64(pingBytes))
	// Send it back
	n, err = stream.Write(pingBytes)
	if err != nil {
		log.Warn("handler crashed", "err", err)
		return
	}
	if n != 8 {
		log.Warn("Could not send Ping")
	}
}

func statusHandler(stream network.Stream) {
	setDeadLines(stream)

	log.Info("Got status request")
}

func goodbyeHandler(stream network.Stream) {
	goodByeBytes := make([]byte, 8)
	setDeadLines(stream)
	n, err := stream.Read(goodByeBytes)
	if err != nil {
		log.Warn("Goodbye handler crashed", "err", err)
		return
	}

	if n != 8 {
		log.Warn("Invalid goodbye message received")
		return
	}

	log.Info("[Lightclient] Received", "goodbye", ssz.UnmarshallUint64(goodByeBytes))
	n, err = stream.Write(goodByeBytes)
	if err != nil {
		log.Warn("Goodbye handler crashed", "err", err)
		return
	}

	if n != 8 {
		log.Warn("Could not send Goodbye")
	}

	peerId := stream.Conn().RemotePeer()
	disconnectPeerCh <- peerId
}

func blocksByRangeHandler(stream network.Stream) {
	setDeadLines(stream)
	log.Info("Got block by range handler call")
}

func beaconBlocksByRootHandler(stream network.Stream) {
	setDeadLines(stream)
	log.Info("Got beacon block by root handler call")
}

func metadataHandler(stream network.Stream) {
	setDeadLines(stream)
	log.Info("Got metadata handler call")
}

func (s *Sentinel) setupHandlers() {
	for id, handler := range handlers {
		s.host.SetStreamHandler(id, handler)
	}
}
