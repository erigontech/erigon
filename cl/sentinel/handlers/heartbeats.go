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
	"github.com/libp2p/go-libp2p/core/network"
)

// Type safe handlers which all have access to the original stream & decompressed data.
// Since packets are just structs, they can be resent with no issue

func (c *ConsensusHandlers) pingHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "ping", defaultRateLimits.pingLimit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		defer s.Close()
		return err
	}
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{
		Id: c.metadata.SeqNumber,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) goodbyeHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "goodbye", defaultRateLimits.goodbyeLimit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		defer s.Close()
		return err
	}
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{
		Id: 1,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) metadataV1Handler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "metadataV1", defaultRateLimits.metadataV1Limit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		defer s.Close()
		return err
	}
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Metadata{
		SeqNumber: c.metadata.SeqNumber,
		Attnets:   c.metadata.Attnets,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) metadataV2Handler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "metadataV2", defaultRateLimits.metadataV2Limit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		defer s.Close()
		return err
	}
	return ssz_snappy.EncodeAndWrite(s, c.metadata, SuccessfulResponsePrefix)
}

// TODO: Actually respond with proper status
func (c *ConsensusHandlers) statusHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "status", defaultRateLimits.statusLimit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		defer s.Close()
		return err
	}
	defer s.Close()
	status := &cltypes.Status{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, status, clparams.Phase0Version); err != nil {
		return err
	}
	return ssz_snappy.EncodeAndWrite(s, status, SuccessfulResponsePrefix)
}
