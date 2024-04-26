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
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
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
