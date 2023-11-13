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
	"context"
	"slices"
	"strings"

	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var rateLimitInBytes = 300 * 1024 // 300 KB
var baseReqSurplus = 10 * 1024    // 10 KB

var rateLimitedProtocolsNames = []string{
	communication.BeaconBlocksByRangeProtocolV2,
	communication.BeaconBlocksByRootProtocolV2,
}

type ConsensusHandlers struct {
	handlers          map[protocol.ID]network.StreamHandler
	host              host.Host
	metadata          *cltypes.Metadata
	beaconConfig      *clparams.BeaconChainConfig
	genesisConfig     *clparams.GenesisConfig
	ctx               context.Context
	bandwidthReporter metrics.Reporter // We use this to report bandwidth usage to the metrics subsystem

	beaconDB persistence.RawBeaconBlockChain
}

const (
	SuccessfulResponsePrefix = 0x00
	RateLimitedPrefix        = 0x02
	ResourceUnavaiablePrefix = 0x03
)

func NewConsensusHandlers(ctx context.Context, db persistence.RawBeaconBlockChain, host host.Host, bandwidthReporter metrics.Reporter,
	peers *peers.Pool, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, metadata *cltypes.Metadata) *ConsensusHandlers {
	c := &ConsensusHandlers{
		host:              host,
		metadata:          metadata,
		beaconDB:          db,
		genesisConfig:     genesisConfig,
		beaconConfig:      beaconConfig,
		ctx:               ctx,
		bandwidthReporter: bandwidthReporter,
	}

	hm := map[string]func(s network.Stream) error{
		communication.PingProtocolV1:                c.pingHandler,
		communication.GoodbyeProtocolV1:             c.goodbyeHandler,
		communication.StatusProtocolV1:              c.statusHandler,
		communication.MetadataProtocolV1:            c.metadataV1Handler,
		communication.MetadataProtocolV2:            c.metadataV2Handler,
		communication.BeaconBlocksByRangeProtocolV2: c.blocksByRangeHandler,
		communication.BeaconBlocksByRootProtocolV2:  c.beaconBlocksByRootHandler,
	}

	c.handlers = map[protocol.ID]network.StreamHandler{}
	for k, v := range hm {
		c.handlers[protocol.ID(k)] = c.wrapStreamHandler(k, v)
	}
	return c
}

func (c *ConsensusHandlers) Start() {
	for id, handler := range c.handlers {
		c.host.SetStreamHandler(id, handler)
	}
}

func (c *ConsensusHandlers) wrapStreamHandler(name string, fn func(s network.Stream) error) func(s network.Stream) {
	return func(s network.Stream) {
		l := log.Ctx{
			"name": name,
		}
		rawVer, err := c.host.Peerstore().Get(s.Conn().RemotePeer(), "AgentVersion")
		if err == nil {
			if str, ok := rawVer.(string); ok {
				l["agent"] = str
			}
		}
		remoteID := s.Conn().RemotePeer()
		if slices.Contains(rateLimitedProtocolsNames, name) {
			stats := c.bandwidthReporter.GetBandwidthForPeer(remoteID)
			if int(stats.RateOut) > rateLimitInBytes {
				if err := ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix); err != nil {
					l["err"] = err
					s.Reset()
					return
				}
				err = s.Close()
				if err != nil {
					l["err"] = err
				}
				return
			}
		}
		err = fn(s)
		if err != nil {
			l["err"] = err
			log.Trace("[pubsubhandler] stream handler", l)
			// TODO: maybe we should log this
			_ = s.Reset()
			return
		}
		err = s.Close()
		if err != nil {
			l["err"] = err
			if !(strings.Contains(name, "goodbye") && (strings.Contains(err.Error(), "session shut down") || strings.Contains(err.Error(), "stream reset"))) {
				log.Trace("[pubsubhandler] close stream", l)
			}
		}
	}
}
