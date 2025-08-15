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
	"encoding/hex"
	"strings"

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
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{
		Id: c.me.Seq(),
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) goodbyeHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	gid := &cltypes.Ping{}
	if s.Conn().IsClosed() {
		return nil
	}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, gid, clparams.Phase0Version); err != nil {
		if strings.Contains(err.Error(), "stream reset") {
			return nil
		}
		return err
	}

	if gid.Id > 250 { // 250 is the status code for getting banned due to whatever reason
		v, err := c.host.Peerstore().Get("AgentVersion", peerId)
		if err == nil {
			log.Warn("Received goodbye message from peer", "v", v)
		}
	}
	// ignore the error from goodbye, we don't care about it
	ssz_snappy.EncodeAndWrite(s, &cltypes.Ping{
		Id: 1,
	}, SuccessfulResponsePrefix)
	return nil
}

func (c *ConsensusHandlers) metadataV1Handler(s network.Stream) error {
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

func (c *ConsensusHandlers) metadataV3Handler(s network.Stream) error {
	if c.ethClock.GetCurrentEpoch() < c.beaconConfig.FuluForkEpoch {
		return nil
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

	cgc := c.peerdasStateReader.GetAdvertisedCgc()
	return ssz_snappy.EncodeAndWrite(s, &cltypes.Metadata{
		SeqNumber:         c.me.Seq(),
		Attnets:           subnetField,
		Syncnets:          &syncnetField,
		CustodyGroupCount: &cgc,
	}, SuccessfulResponsePrefix)
}

// TODO: Actually respond with proper status
func (c *ConsensusHandlers) statusHandler(s network.Stream) error {
	return ssz_snappy.EncodeAndWrite(s, c.hs.Status(), SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) statusV2Handler(s network.Stream) error {
	status := c.hs.Status()
	log.Debug("statusV2Handler", "forkDigest", hex.EncodeToString(status.ForkDigest[:]), "finalizedRoot", hex.EncodeToString(status.FinalizedRoot[:]),
		"finalizedEpoch", status.FinalizedEpoch, "headSlot", status.HeadSlot, "headRoot", hex.EncodeToString(status.HeadRoot[:]), "earliestAvailableSlot", c.peerdasStateReader.GetEarliestAvailableSlot())
	return ssz_snappy.EncodeAndWrite(s, status, SuccessfulResponsePrefix)
}
