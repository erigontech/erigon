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
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/erigontech/erigon/cl/clparams"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/handshake"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/p2p/enode"
)

var (
	ErrResourceUnavailable = errors.New("resource unavailable")
)

type ConsensusHandlers struct {
	handlers     map[protocol.ID]network.StreamHandler
	hs           *handshake.HandShaker
	beaconConfig *clparams.BeaconChainConfig
	ethClock     eth_clock.EthereumClock
	ctx          context.Context
	beaconDB     freezeblocks.BeaconSnapshotReader

	indiciesDB         kv.RoDB
	rateLimiter        *peerRateLimiter
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
		rateLimiter:        newPeerRateLimiter(),
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

func (c *ConsensusHandlers) Start() {
	c.rateLimiter.startCleanup(c.ctx)
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

		peerID := s.Conn().RemotePeer().String()

		// Enforce per-peer concurrent stream cap.
		if !c.rateLimiter.acquireConcurrency(peerID) {
			log.Debug("[pubsubhandler] too many concurrent requests", "protocol", name, "peer", peerID)
			_ = ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
			_ = s.Close() // graceful close so the peer reads the error response
			return
		}
		defer c.rateLimiter.releaseConcurrency(peerID)

		// Enforce per-peer, per-protocol rate limit (1 token for admission;
		// batch handlers consume additional tokens after decoding the request).
		if !c.rateLimiter.allowRequest(peerID, name, 1) {
			log.Debug("[pubsubhandler] rate limit exceeded", "protocol", name, "peer", peerID)
			_ = ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
			_ = s.Close() // graceful close so the peer reads the error response
			return
		}

		l := log.Ctx{
			"name": name,
		}
		rawVer, err := c.host.Peerstore().Get(s.Conn().RemotePeer(), "AgentVersion")
		if err == nil {
			if str, ok := rawVer.(string); ok {
				l["agent"] = str
			}
		}

		streamDeadline := time.Now().Add(5 * time.Second)
		s.SetReadDeadline(streamDeadline)
		s.SetWriteDeadline(streamDeadline)
		s.SetDeadline(streamDeadline)

		if err := fn(s); err != nil {
			if errors.Is(err, ErrResourceUnavailable) {
				// write resource unavailable prefix
				if _, err := s.Write([]byte{ResourceUnavailablePrefix}); err != nil {
					log.Debug("failed to write resource unavailable prefix", "err", err)
				}
			}
			log.Trace("[pubsubhandler] stream handler returned error", "protocol", name, "peer", s.Conn().RemotePeer().String(), "err", err)
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

// consumeRateLimit consumes additional rate-limit tokens for a batch request.
// Batch handlers call this after decoding the request to charge for the actual
// number of response items (beyond the 1 token already consumed by the wrapper).
// Returns true if allowed. On rejection, writes an SSZ error response to s.
func (c *ConsensusHandlers) consumeRateLimit(s network.Stream, cost int) bool {
	if cost <= 0 {
		return true
	}
	peerID := s.Conn().RemotePeer().String()
	protocol := string(s.Protocol())
	if !c.rateLimiter.consumeTokens(peerID, protocol, cost) {
		log.Debug("[pubsubhandler] rate limit exceeded (batch)", "protocol", protocol, "peer", peerID, "cost", cost)
		_ = ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
		return false
	}
	return true
}
