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
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
)

func (c *ConsensusHandlers) lightClientFinalityUpdateHandler(stream network.Stream) {
	if c.db == nil {
		stream.Write([]byte{ResourceUnavaiablePrefix})
		return
	}

	forkDigest, err := fork.ComputeForkDigest(c.beaconConfig, c.genesisConfig)
	if err != nil {
		stream.Close()
		return
	}
	if _, err := stream.Write(forkDigest[:]); err != nil {
		stream.Close()
		return
	}
	// Read latest lightclient update
	tx, err := c.db.BeginRo(c.ctx)
	if err != nil {
		stream.Close()
		return
	}
	defer tx.Rollback()
	update, err := rawdb.ReadLightClientFinalityUpdate(tx)
	if err != nil || update == nil {
		stream.Close()
		return
	}

	ssz_snappy.EncodeAndWrite(stream, update, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) lightClientOptimisticUpdateHandler(stream network.Stream) {
	if c.db == nil {
		stream.Write([]byte{ResourceUnavaiablePrefix})
		return
	}
	forkDigest, err := fork.ComputeForkDigest(c.beaconConfig, c.genesisConfig)
	if err != nil {
		stream.Close()
		return
	}
	if _, err := stream.Write(forkDigest[:]); err != nil {
		stream.Close()
		return
	}
	// Read latest lightclient update
	tx, err := c.db.BeginRo(c.ctx)
	if err != nil {
		stream.Close()
		return
	}
	defer tx.Rollback()
	update, err := rawdb.ReadLightClientOptimisticUpdate(tx)
	if err != nil || update == nil {
		stream.Close()
		return
	}

	ssz_snappy.EncodeAndWrite(stream, update, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) lightClientUpdatesByRange(stream network.Stream) {
	if c.db == nil {
		stream.Write([]byte{ResourceUnavaiablePrefix})
		return
	}
	log.Info("Got lightClientUpdatesByRange handler call")
	// TODO: ADD Proper logic
}
