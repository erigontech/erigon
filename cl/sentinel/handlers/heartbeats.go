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
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/p2p/enr"
)

// Type safe handlers which all have access to the original stream & decompressed data.
// Since packets are just structs, they can be resent with no issue

func (c *ConsensusHandlers) pingHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "ping", rateLimits.pingLimit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{
		Id: c.me.Seq(),
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) goodbyeHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "goodbye", rateLimits.goodbyeLimit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}
	gid := &cltypes.Ping{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, gid, clparams.Phase0Version); err != nil {
		return err
	}

	if gid.Id > 250 { // 250 is the status code for getting banned due to whatever reason
		v, err := c.host.Peerstore().Get("AgentVersion", peerId)
		if err == nil {
			log.Debug("Received goodbye message from peer", "v", v)
		}
	}

	return ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{
		Id: 1,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) metadataV1Handler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "metadataV1", rateLimits.metadataV1Limit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}
	subnetField := [8]byte{}
	attSubEnr := enr.WithEntry(c.netCfg.AttSubnetKey, &subnetField)
	if err := c.me.Node().Load(attSubEnr); err != nil {
		return err
	}
	//me.Load()
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Metadata{
		SeqNumber: c.me.Seq(),
		Attnets:   subnetField,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) metadataV2Handler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()

	if err := c.checkRateLimit(peerId, "metadataV2", rateLimits.metadataV2Limit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}
	subnetField := [8]byte{}
	syncnetField := [1]byte{}
	attSubEnr := enr.WithEntry(c.netCfg.AttSubnetKey, &subnetField)
	syncNetEnr := enr.WithEntry(c.netCfg.SyncCommsSubnetKey, &syncnetField)
	if err := c.me.Node().Load(attSubEnr); err != nil {
		return err
	}
	if err := c.me.Node().Load(syncNetEnr); err != nil {
		return err
	}

	return ssz_snappy.EncodeAndWrite(s, &cltypes.Metadata{
		SeqNumber: c.me.Seq(),
		Attnets:   subnetField,
		Syncnets:  &syncnetField,
	}, SuccessfulResponsePrefix)
}

// TODO: Actually respond with proper status
func (c *ConsensusHandlers) statusHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "status", rateLimits.statusLimit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	return ssz_snappy.EncodeAndWrite(s, c.hs.Status(), SuccessfulResponsePrefix)
}
