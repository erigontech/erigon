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

package handlers

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/peers"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/ssz_snappy"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var ProtocolPrefix = "/eth2/beacon_chain/req"

type ConsensusHandlers struct {
	handlers map[protocol.ID]network.StreamHandler
	host     host.Host
	peers    *peers.Peers
}

func NewConsensusHandlers(host host.Host, peers *peers.Peers) *ConsensusHandlers {
	c := &ConsensusHandlers{
		peers: peers,
		host:  host,
	}
	c.handlers = map[protocol.ID]network.StreamHandler{
		protocol.ID(ProtocolPrefix + "/ping/1/ssz_snappy"):                   curryStreamHandler(ssz_snappy.NewStreamCodec, pingHandler),
		protocol.ID(ProtocolPrefix + "/status/1/ssz_snappy"):                 curryStreamHandler(ssz_snappy.NewStreamCodec, statusHandler),
		protocol.ID(ProtocolPrefix + "/goodbye/1/ssz_snappy"):                curryStreamHandler(ssz_snappy.NewStreamCodec, c.goodbyeHandler),
		protocol.ID(ProtocolPrefix + "/metadata/1/ssz_snappy"):               curryStreamHandler(ssz_snappy.NewStreamCodec, metadataHandler),
		protocol.ID(ProtocolPrefix + "/beacon_blocks_by_range/1/ssz_snappy"): c.blocksByRangeHandler,
		protocol.ID(ProtocolPrefix + "/beacon_blocks_by_root/1/ssz_snappy"):  c.beaconBlocksByRootHandler,
	}
	return c
}

func (c *ConsensusHandlers) blocksByRangeHandler(stream network.Stream) {
	log.Info("Got block by range handler call")
	defer stream.Close()
}

func (c *ConsensusHandlers) beaconBlocksByRootHandler(stream network.Stream) {
	log.Info("Got beacon block by root handler call")
	defer stream.Close()
}

func (c *ConsensusHandlers) Start() {
	for id, handler := range c.handlers {
		c.host.SetStreamHandler(id, handler)
	}
}
