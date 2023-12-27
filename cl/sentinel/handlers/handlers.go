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
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"golang.org/x/time/rate"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type RateLimits struct {
	pingLimit                int
	goodbyeLimit             int
	metadataV1Limit          int
	metadataV2Limit          int
	statusLimit              int
	beaconBlocksByRangeLimit int
	beaconBlocksByRootLimit  int
}

const punishmentPeriod = time.Minute
const defaultRateLimit = math.MaxInt
const defaultBlockHandlerRateLimit = 200

var rateLimits = RateLimits{
	pingLimit:                defaultRateLimit,
	goodbyeLimit:             defaultRateLimit,
	metadataV1Limit:          defaultRateLimit,
	metadataV2Limit:          defaultRateLimit,
	statusLimit:              defaultRateLimit,
	beaconBlocksByRangeLimit: defaultBlockHandlerRateLimit,
	beaconBlocksByRootLimit:  defaultBlockHandlerRateLimit,
}

type ConsensusHandlers struct {
	handlers           map[protocol.ID]network.StreamHandler
	host               host.Host
	metadata           *cltypes.Metadata
	beaconConfig       *clparams.BeaconChainConfig
	genesisConfig      *clparams.GenesisConfig
	ctx                context.Context
	beaconDB           persistence.RawBeaconBlockChain
	indiciesDB         kv.RoDB
	peerRateLimits     sync.Map
	punishmentEndTimes sync.Map

	enableBlocks bool
}

const (
	SuccessfulResponsePrefix = 0x00
	RateLimitedPrefix        = 0x01
	ResourceUnavaiablePrefix = 0x02
)

func NewConsensusHandlers(ctx context.Context, db persistence.RawBeaconBlockChain, indiciesDB kv.RoDB, host host.Host,
	peers *peers.Pool, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, metadata *cltypes.Metadata, enabledBlocks bool) *ConsensusHandlers {
	c := &ConsensusHandlers{
		host:               host,
		metadata:           metadata,
		beaconDB:           db,
		indiciesDB:         indiciesDB,
		genesisConfig:      genesisConfig,
		beaconConfig:       beaconConfig,
		ctx:                ctx,
		peerRateLimits:     sync.Map{},
		punishmentEndTimes: sync.Map{},
		enableBlocks:       enabledBlocks,
	}

	hm := map[string]func(s network.Stream) error{
		communication.PingProtocolV1:     c.pingHandler,
		communication.GoodbyeProtocolV1:  c.goodbyeHandler,
		communication.StatusProtocolV1:   c.statusHandler,
		communication.MetadataProtocolV1: c.metadataV1Handler,
		communication.MetadataProtocolV2: c.metadataV2Handler,
	}

	if c.enableBlocks {
		hm[communication.BeaconBlocksByRangeProtocolV2] = c.beaconBlocksByRangeHandler
		hm[communication.BeaconBlocksByRootProtocolV2] = c.beaconBlocksByRootHandler
	}

	c.handlers = map[protocol.ID]network.StreamHandler{}
	for k, v := range hm {
		c.handlers[protocol.ID(k)] = c.wrapStreamHandler(k, v)
	}
	return c
}

func (c *ConsensusHandlers) checkRateLimit(peerId string, method string, limit int) error {
	keyHash := utils.Sha256([]byte(peerId), []byte(method))

	if punishmentEndTime, ok := c.punishmentEndTimes.Load(keyHash); ok {
		if time.Now().Before(punishmentEndTime.(time.Time)) {
			return errors.New("rate limit exceeded, punishment period in effect")
		}
		c.punishmentEndTimes.Delete(keyHash)
	}

	value, ok := c.peerRateLimits.Load(keyHash)
	if !ok {
		value = rate.NewLimiter(rate.Every(time.Minute), limit)
		c.peerRateLimits.Store(keyHash, value)
	}

	limiter := value.(*rate.Limiter)

	if !limiter.Allow() {
		c.punishmentEndTimes.Store(keyHash, time.Now().Add(punishmentPeriod))
		c.peerRateLimits.Delete(keyHash)
		return errors.New("rate limit exceeded")
	}

	return nil
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
		err = fn(s)
		if err != nil {
			l["err"] = err
			log.Trace("[pubsubhandler] stream handler", l)
			// TODO: maybe we should log this
			_ = s.Reset()
			_ = s.Close()
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
