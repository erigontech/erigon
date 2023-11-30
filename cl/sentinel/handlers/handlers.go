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
	"strings"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type RateLimiter struct {
	counter         uint32
	lastRequestTime time.Time
	limited         bool
}

type RateLimits struct {
	pingLimit       uint32
	goodbyeLimit    uint32
	metadataV1Limit uint32
	metadataV2Limit uint32
	statusLimit     uint32
}

const punishmentPeriod = 15 //mins

var defaultRateLimits = RateLimits{
	pingLimit:       5000,
	goodbyeLimit:    5000,
	metadataV1Limit: 5000,
	metadataV2Limit: 5000,
	statusLimit:     5000,
}

type ConsensusHandlers struct {
	handlers       map[protocol.ID]network.StreamHandler
	host           host.Host
	metadata       *cltypes.Metadata
	beaconConfig   *clparams.BeaconChainConfig
	genesisConfig  *clparams.GenesisConfig
	ctx            context.Context
	beaconDB       persistence.RawBeaconBlockChain
	peerRateLimits map[string]map[string]*RateLimiter
}

const (
	SuccessfulResponsePrefix = 0x00
	ResourceUnavaiablePrefix = 0x03
)

func NewConsensusHandlers(ctx context.Context, db persistence.RawBeaconBlockChain, host host.Host,
	peers *peers.Pool, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, metadata *cltypes.Metadata) *ConsensusHandlers {
	c := &ConsensusHandlers{
		host:           host,
		metadata:       metadata,
		beaconDB:       db,
		genesisConfig:  genesisConfig,
		beaconConfig:   beaconConfig,
		ctx:            ctx,
		peerRateLimits: make(map[string]map[string]*RateLimiter),
	}

	hm := map[string]func(s network.Stream) error{
		communication.PingProtocolV1:                c.pingHandler,
		communication.GoodbyeProtocolV1:             c.goodbyeHandler,
		communication.StatusProtocolV1:              c.statusHandler,
		communication.MetadataProtocolV1:            c.metadataV1Handler,
		communication.MetadataProtocolV2:            c.metadataV2Handler,
		communication.BeaconBlocksByRangeProtocolV1: c.blocksByRangeHandler,
		communication.BeaconBlocksByRootProtocolV1:  c.beaconBlocksByRootHandler,
	}

	c.handlers = map[protocol.ID]network.StreamHandler{}
	for k, v := range hm {
		c.handlers[protocol.ID(k)] = c.wrapStreamHandler(k, v)
	}
	return c
}

func (limiter *RateLimiter) incrementCounter() {
	atomic.AddUint32(&limiter.counter, 1)
}

func (limiter *RateLimiter) resetCounter() {
	atomic.StoreUint32(&limiter.counter, 0)
}

func (limiter *RateLimiter) loadCounter() uint32 {
	return atomic.LoadUint32(&limiter.counter)
}

func (c *ConsensusHandlers) checkRateLimit(peerId string, method string, limit uint32) error {
	// Initialize
	if _, ok := c.peerRateLimits[peerId]; !ok {
		c.peerRateLimits[peerId] = make(map[string]*RateLimiter)
	}
	if _, ok := c.peerRateLimits[peerId][method]; !ok {
		c.peerRateLimits[peerId][method] = &RateLimiter{counter: 0, lastRequestTime: time.Now(), limited: false}
	}

	limiter := c.peerRateLimits[peerId][method]

	// Check if the peer is in the punishment period
	now := time.Now()
	if limiter.limited {
		punishmentEndTime := limiter.lastRequestTime.Add(punishmentPeriod * time.Minute)
		if now.After(punishmentEndTime) {
			limiter.limited = false
		} else {
			return errors.New("rate limited")
		}
	}

	// reset counter after 1 minute
	afterMinute := limiter.lastRequestTime.Add(time.Minute)
	if now.After(afterMinute) {
		limiter.resetCounter()
	}

	limiter.incrementCounter()
	limiter.lastRequestTime = now

	// Check the rate limit
	if limiter.loadCounter() >= limit {
		limiter.limited = true
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
