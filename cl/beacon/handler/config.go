// Copyright 2024 The Erigon Authors
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

package handler

import (
	"bytes"
	"cmp"
	"net/http"
	"slices"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/utils"
)

func (a *ApiHandler) getSpec(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	outSpec := struct {
		*clparams.BeaconChainConfig
		*clparams.NetworkConfig
		MaxPayloadSize                   uint64                       `json:"MAX_PAYLOAD_SIZE,string"`
		MaximumGossipClockDisparity      *clparams.ConfigDurationMSec `json:"MAXIMUM_GOSSIP_CLOCK_DISPARITY"`
		PendingDepositsLimit             uint64                       `json:"PENDING_DEPOSITS_LIMIT,string"`
		MaxAttesterSlashingSize          uint64                       `json:"MAX_ATTESTER_SLASHING_SIZE,string"`
		MaxSignedAggregateAndProofSize   uint64                       `json:"MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE,string"`
		MaxSignedExecutionPayloadBidSize uint64                       `json:"MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE,string"`
		UpdateTimeout                    uint64                       `json:"UPDATE_TIMEOUT,string"`
		SlotDurationMs                   uint64                       `json:"SLOT_DURATION_MS,string"`
		MinEpochsForBlockRequests        uint64                       `json:"MIN_EPOCHS_FOR_BLOCK_REQUESTS,string"`
	}{
		BeaconChainConfig:                a.beaconChainCfg,
		NetworkConfig:                    a.netConfig,
		MaxPayloadSize:                   a.netConfig.GossipMaxSize,
		MaximumGossipClockDisparity:      &a.netConfig.MaximumGossipClockDisparity,
		PendingDepositsLimit:             a.beaconChainCfg.PendingDepositLimits,
		MaxAttesterSlashingSize:          gossip.MaxUncompressedSize(gossip.TopicNameAttesterSlashing, a.beaconChainCfg, a.netConfig),
		MaxSignedAggregateAndProofSize:   gossip.MaxUncompressedSize(gossip.TopicNameBeaconAggregateAndProof, a.beaconChainCfg, a.netConfig),
		MaxSignedExecutionPayloadBidSize: uint64(maxSignedExecutionPayloadBidSSZSize()),
		UpdateTimeout:                    a.beaconChainCfg.SlotsPerEpoch * a.beaconChainCfg.EpochsPerSyncCommitteePeriod,
		SlotDurationMs:                   a.beaconChainCfg.SecondsPerSlot * 1000,
		MinEpochsForBlockRequests:        a.beaconChainCfg.MinEpochsForBlockRequests(),
	}

	return newBeaconResponse(outSpec), nil
}

func (a *ApiHandler) getDepositContract(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(struct {
		ChainId         uint64 `json:"chain_id,string"`
		DepositContract string `json:"address"`
	}{ChainId: a.beaconChainCfg.DepositChainID, DepositContract: a.beaconChainCfg.DepositContractAddress}), nil
}

func (a *ApiHandler) getForkSchedule(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	response := make([]cltypes.Fork, 0, len(a.beaconChainCfg.ForkVersionSchedule))
	// create first response (unordered and incomplete)
	for currentVersion, entry := range a.beaconChainCfg.ForkVersionSchedule {
		response = append(response, cltypes.Fork{
			CurrentVersion: currentVersion,
			Epoch:          entry.Epoch,
		})
	}
	// Sort the responses by epoch
	slices.SortFunc(response, func(a, b cltypes.Fork) int {
		if a.Epoch == b.Epoch {
			return bytes.Compare(a.CurrentVersion[:], b.CurrentVersion[:])
		}
		return cmp.Compare(a.Epoch, b.Epoch)
	})
	previousVersion := utils.Uint32ToBytes4(uint32(a.beaconChainCfg.GenesisForkVersion))
	for i := range response {
		response[i].PreviousVersion = previousVersion
		previousVersion = response[i].CurrentVersion
	}
	return newBeaconResponse(response), nil
}
