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
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/peers"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type ConsensusHandlers struct {
	handlers map[protocol.ID]network.StreamHandler
	host     host.Host
	peers    *peers.Peers
	metadata *cltypes.MetadataV2
}

const SuccessfullResponsePrefix = 0x00

var NoRequestHandlers = map[string]bool{
	MetadataProtocolV1:          true,
	MetadataProtocolV2:          true,
	LightClientFinalityUpdateV1: true,
}

func NewConsensusHandlers(host host.Host, peers *peers.Peers, metadata *cltypes.MetadataV2) *ConsensusHandlers {
	c := &ConsensusHandlers{
		peers:    peers,
		host:     host,
		metadata: metadata,
	}
	c.handlers = map[protocol.ID]network.StreamHandler{
		protocol.ID(PingProtocolV1):               curryStreamHandler(ssz_snappy.NewStreamCodec, pingHandler),
		protocol.ID(GoodbyeProtocolV1):            curryStreamHandler(ssz_snappy.NewStreamCodec, pingHandler),
		protocol.ID(StatusProtocolV1):             curryStreamHandler(ssz_snappy.NewStreamCodec, statusHandler),
		protocol.ID(MetadataProtocolV1):           curryStreamHandler(ssz_snappy.NewStreamCodec, c.metadataV1Handler),
		protocol.ID(MetadataProtocolV2):           curryStreamHandler(ssz_snappy.NewStreamCodec, c.metadataV2Handler),
		protocol.ID(BeaconBlockByRangeProtocolV1): c.blocksByRangeHandler,
		protocol.ID(BeaconBlockByRootProtocolV1):  c.beaconBlocksByRootHandler,
	}
	return c
}

func (c *ConsensusHandlers) blocksByRangeHandler(stream network.Stream) {
	log.Info("Got block by range handler call")
}

func (c *ConsensusHandlers) beaconBlocksByRootHandler(stream network.Stream) {
	log.Info("Got beacon block by root handler call")
}

func (c *ConsensusHandlers) Start() {
	for id, handler := range c.handlers {
		c.host.SetStreamHandler(id, handler)
	}
}
