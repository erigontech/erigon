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
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/handshake"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/p2p/enode"
)

var (
	ErrResourceUnavailable = errors.New("resource unavailable")
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

type ConsensusHandlers struct {
	handlers     map[protocol.ID]network.StreamHandler
	hs           *handshake.HandShaker
	beaconConfig *clparams.BeaconChainConfig
	ethClock     eth_clock.EthereumClock
	ctx          context.Context
	beaconDB     freezeblocks.BeaconSnapshotReader

	indiciesDB         kv.RoDB
	punishmentEndTimes sync.Map
	forkChoiceReader   forkchoice.ForkChoiceStorageReader
	host               host.Host
	me                 *enode.LocalNode
	netCfg             *clparams.NetworkConfig
	blobsStorage       blob_storage.BlobStorage
	dataColumnStorage  blob_storage.DataColumnStorage
	peerdasStateReader peerdasstate.PeerDasStateReader
	enableBlocks       bool
}

const (
	SuccessfulResponsePrefix  = 0x00
	InvalidRequestPrefix      = 0x01
	ServerErrorPrefix         = 0x02
	ResourceUnavailablePrefix = 0x03
)

func NewConsensusHandlers(
	ctx context.Context,
	db freezeblocks.BeaconSnapshotReader,
	indiciesDB kv.RoDB,
	host host.Host,
	peers *peers.Pool,
	netCfg *clparams.NetworkConfig,
	me *enode.LocalNode,
	beaconConfig *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	hs *handshake.HandShaker,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	blobsStorage blob_storage.BlobStorage,
	dataColumnStorage blob_storage.DataColumnStorage,
	peerDasStateReader peerdasstate.PeerDasStateReader,
	enabledBlocks bool) *ConsensusHandlers {
	c := &ConsensusHandlers{
		host:               host,
		hs:                 hs,
		beaconDB:           db,
		indiciesDB:         indiciesDB,
		ethClock:           ethClock,
		beaconConfig:       beaconConfig,
		ctx:                ctx,
		punishmentEndTimes: sync.Map{},
		enableBlocks:       enabledBlocks,
		forkChoiceReader:   forkChoiceReader,
		me:                 me,
		netCfg:             netCfg,
		blobsStorage:       blobsStorage,
		dataColumnStorage:  dataColumnStorage,
		peerdasStateReader: peerDasStateReader,
	}

	hm := map[string]func(s network.Stream) error{
		communication.PingProtocolV1:                        c.pingHandler,
		communication.GoodbyeProtocolV1:                     c.goodbyeHandler,
		communication.StatusProtocolV1:                      c.statusHandler,
		communication.StatusProtocolV2:                      c.statusV2Handler,
		communication.MetadataProtocolV1:                    c.metadataV1Handler,
		communication.MetadataProtocolV2:                    c.metadataV2Handler,
		communication.MetadataProtocolV3:                    c.metadataV3Handler,
		communication.LightClientOptimisticUpdateProtocolV1: c.optimisticLightClientUpdateHandler,
		communication.LightClientFinalityUpdateProtocolV1:   c.finalityLightClientUpdateHandler,
		communication.LightClientBootstrapProtocolV1:        c.lightClientBootstrapHandler,
		communication.LightClientUpdatesByRangeProtocolV1:   c.lightClientUpdatesByRangeHandler,
	}

	if c.enableBlocks {
		hm[communication.BeaconBlocksByRangeProtocolV2] = c.beaconBlocksByRangeHandler
		hm[communication.BeaconBlocksByRootProtocolV2] = c.beaconBlocksByRootHandler
		// blobs
		hm[communication.BlobSidecarByRangeProtocolV1] = c.blobsSidecarsByRangeHandlerDeneb
		hm[communication.BlobSidecarByRootProtocolV1] = c.blobsSidecarsByIdsHandlerDeneb
		// data column sidecars
		hm[communication.DataColumnSidecarsByRangeProtocolV1] = c.dataColumnSidecarsByRangeHandler
		hm[communication.DataColumnSidecarsByRootProtocolV1] = c.dataColumnSidecarsByRootHandler
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

	return nil
}

func (c *ConsensusHandlers) Start() {
	for id, handler := range c.handlers {
		c.host.SetStreamHandler(id, handler)
	}
}

func (c *ConsensusHandlers) wrapStreamHandler(name string, fn func(s network.Stream) error) func(s network.Stream) {
	return func(s network.Stream) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("[pubsubhandler] panic in stream handler", "err", r, "name", name)
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
		if err := fn(s); err != nil {
			if errors.Is(err, ErrResourceUnavailable) {
				// write resource unavailable prefix
				if _, err := s.Write([]byte{ResourceUnavailablePrefix}); err != nil {
					log.Debug("failed to write resource unavailable prefix", "err", err)
				}
			}
			log.Debug("[pubsubhandler] stream handler returned error", "protocol", name, "peer", s.Conn().RemotePeer().String(), "err", err)
			_ = s.Reset()
			_ = s.Close()
			return
		}
		if err := s.Close(); err != nil {
			l["err"] = err
			if !(strings.Contains(name, "goodbye") &&
				(strings.Contains(err.Error(), "session shut down") ||
					strings.Contains(err.Error(), "stream reset"))) {
				log.Trace("[pubsubhandler] close stream", l)
			}
		}
	}
}
