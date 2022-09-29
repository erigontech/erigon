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
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	ssz "github.com/prysmaticlabs/fastssz"
)

var ProtocolPrefix = "/eth2/beacon_chain/req"

func getHandlers(s *Sentinel) map[protocol.ID]network.StreamHandler {
	return map[protocol.ID]network.StreamHandler{
		protocol.ID(ProtocolPrefix + "/ping/1/ssz_snappy"):                   s.pingHandler,
		protocol.ID(ProtocolPrefix + "/status/1/ssz_snappy"):                 s.statusHandler,
		protocol.ID(ProtocolPrefix + "/goodbye/1/ssz_snappy"):                s.goodbyeHandler,
		protocol.ID(ProtocolPrefix + "/beacon_blocks_by_range/1/ssz_snappy"): s.blocksByRangeHandler,
		protocol.ID(ProtocolPrefix + "/beacon_blocks_by_root/1/ssz_snappy"):  s.beaconBlocksByRootHandler,
		protocol.ID(ProtocolPrefix + "/metadata/1/ssz_snappy"):               s.metadataHandler,
	}
}

func (s *Sentinel) pingHandler(stream network.Stream) {
	pingBytes := make([]byte, 8)
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

func (s *Sentinel) statusHandler(stream network.Stream) {

	log.Info("Got status request")
}

func (s *Sentinel) goodbyeHandler(stream network.Stream) {
	goodByeBytes := make([]byte, 8)
	n, err := stream.Read(goodByeBytes)
	if err == network.ErrReset {
		// Handle data race.
		return
	}
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
	if err == network.ErrReset {
		// We disconnected prior so this error can be ignored.
		return
	}
	if err != nil {
		log.Warn("Goodbye handler crashed", "err", err)
		return
	}

	if n != 8 {
		log.Warn("Could not send Goodbye")
	}

	s.peers.DisconnectPeer(stream.Conn().RemotePeer())
}

func (s *Sentinel) blocksByRangeHandler(stream network.Stream) {
	log.Info("Got block by range handler call")
}

func (s *Sentinel) beaconBlocksByRootHandler(stream network.Stream) {
	log.Info("Got beacon block by root handler call")
}

func (s *Sentinel) metadataHandler(stream network.Stream) {
	log.Info("Got metadata handler call")
}

func (s *Sentinel) setupHandlers() {
	for id, handler := range getHandlers(s) {
		s.host.SetStreamHandler(id, handler)
	}
}
