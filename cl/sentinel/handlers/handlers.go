// Copyright 2022 The Erigon Authors
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

package handlers

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/v3/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/v3/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/v3/cl/sentinel/communication"
	"github.com/erigontech/erigon/v3/cl/sentinel/handshake"
	"github.com/erigontech/erigon/v3/cl/sentinel/peers"
	"github.com/erigontech/erigon/v3/cl/utils"
	"github.com/erigontech/erigon/v3/cl/utils/eth_clock"
	"github.com/erigontech/erigon/v3/p2p/enode"
	"github.com/erigontech/erigon/v3/turbo/snapshotsync/freezeblocks"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/v3/cl/clparams"
)

type RateLimits struct {
	pingLimit                int
	goodbyeLimit             int
	metadataV1Limit          int
	metadataV2Limit          int
	statusLimit              int
	beaconBlocksByRangeLimit int
	beaconBlocksByRootLimit  int
	lightClientLimit         int
	blobSidecarsLimit        int
}

const (
	punishmentPeriod      = time.Minute
	heartBeatRateLimit    = math.MaxInt
	blockHandlerRateLimit = 200
	lightClientRateLimit  = 500
	blobHandlerRateLimit  = 50 // very generous here.
)

var rateLimits = RateLimits{
	pingLimit:                heartBeatRateLimit,
	goodbyeLimit:             heartBeatRateLimit,
	metadataV1Limit:          heartBeatRateLimit,
	metadataV2Limit:          heartBeatRateLimit,
	statusLimit:              heartBeatRateLimit,
	beaconBlocksByRangeLimit: blockHandlerRateLimit,
	beaconBlocksByRootLimit:  blockHandlerRateLimit,
	lightClientLimit:         lightClientRateLimit,
	blobSidecarsLimit:        blobHandlerRateLimit,
}

type ConsensusHandlers struct {
	handlers     map[protocol.ID]network.StreamHandler
	hs           *handshake.HandShaker
	beaconConfig *clparams.BeaconChainConfig
	ethClock     eth_clock.EthereumClock
	ctx          context.Context
	beaconDB     freezeblocks.BeaconSnapshotReader

	indiciesDB         kv.RoDB
	peerRateLimits     sync.Map
	punishmentEndTimes sync.Map
	forkChoiceReader   forkchoice.ForkChoiceStorageReader
	host               host.Host
	me                 *enode.LocalNode
	netCfg             *clparams.NetworkConfig
	blobsStorage       blob_storage.BlobStorage

	enableBlocks bool
}

const (
	SuccessfulResponsePrefix  = 0x00
	RateLimitedPrefix         = 0x01
	ResourceUnavailablePrefix = 0x02
)

func NewConsensusHandlers(ctx context.Context, db freezeblocks.BeaconSnapshotReader, indiciesDB kv.RoDB, host host.Host,
	peers *peers.Pool, netCfg *clparams.NetworkConfig, me *enode.LocalNode, beaconConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock, hs *handshake.HandShaker, forkChoiceReader forkchoice.ForkChoiceStorageReader, blobsStorage blob_storage.BlobStorage, enabledBlocks bool) *ConsensusHandlers {
	c := &ConsensusHandlers{
		host:               host,
		hs:                 hs,
		beaconDB:           db,
		indiciesDB:         indiciesDB,
		ethClock:           ethClock,
		beaconConfig:       beaconConfig,
		ctx:                ctx,
		peerRateLimits:     sync.Map{},
		punishmentEndTimes: sync.Map{},
		enableBlocks:       enabledBlocks,
		forkChoiceReader:   forkChoiceReader,
		me:                 me,
		netCfg:             netCfg,
		blobsStorage:       blobsStorage,
	}

	hm := map[string]func(s network.Stream) error{
		communication.PingProtocolV1:                        c.pingHandler,
		communication.GoodbyeProtocolV1:                     c.goodbyeHandler,
		communication.StatusProtocolV1:                      c.statusHandler,
		communication.MetadataProtocolV1:                    c.metadataV1Handler,
		communication.MetadataProtocolV2:                    c.metadataV2Handler,
		communication.LightClientOptimisticUpdateProtocolV1: c.optimisticLightClientUpdateHandler,
		communication.LightClientFinalityUpdateProtocolV1:   c.finalityLightClientUpdateHandler,
		communication.LightClientBootstrapProtocolV1:        c.lightClientBootstrapHandler,
		communication.LightClientUpdatesByRangeProtocolV1:   c.lightClientUpdatesByRangeHandler,
	}

	if c.enableBlocks {
		hm[communication.BeaconBlocksByRangeProtocolV2] = c.beaconBlocksByRangeHandler
		hm[communication.BeaconBlocksByRootProtocolV2] = c.beaconBlocksByRootHandler
		hm[communication.BlobSidecarByRangeProtocolV1] = c.blobsSidecarsByRangeHandler
		hm[communication.BlobSidecarByRootProtocolV1] = c.blobsSidecarsByIdsHandler
	}

	c.handlers = map[protocol.ID]network.StreamHandler{}
	for k, v := range hm {
		c.handlers[protocol.ID(k)] = c.wrapStreamHandler(k, v)
	}
	return c
}

func (c *ConsensusHandlers) checkRateLimit(peerId string, method string, limit, n int) error {
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

	if !limiter.AllowN(time.Now(), n) {
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
		// handle panic
		defer func() {
			if r := recover(); r != nil {
				log.Error("[pubsubhandler] panic in stream handler", "err", r)
				_ = s.Reset()
				_ = s.Close()
			}
		}()
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
