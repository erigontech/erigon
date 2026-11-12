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
	"github.com/erigontech/erigon/execution/protocol/params"
)

func (a *ApiHandler) getSpec(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	outSpec := struct {
		*clparams.BeaconChainConfig
		*clparams.NetworkConfig
		AttestationSubnetExtraBits        uint64 `json:"ATTESTATION_SUBNET_EXTRA_BITS,string"`
		EpochsPerSubnetSubscription       uint64 `json:"EPOCHS_PER_SUBNET_SUBSCRIPTION,string"`
		ReorgParentWeightThreshold        uint64 `json:"REORG_PARENT_WEIGHT_THRESHOLD,string"`
		ReorgMaxEpochsSinceFinalization   uint64 `json:"REORG_MAX_EPOCHS_SINCE_FINALIZATION,string"`
		ProposerReorgCutoffBps            uint64 `json:"PROPOSER_REORG_CUTOFF_BPS,string"`
		ConfirmationByzantineThreshold    uint64 `json:"CONFIRMATION_BYZANTINE_THRESHOLD,string"`
		CellsPerExtBlob                   uint64 `json:"CELLS_PER_EXT_BLOB,string"`
		FieldElementsPerCell              uint64 `json:"FIELD_ELEMENTS_PER_CELL,string"`
		FieldElementsPerExtBlob           uint64 `json:"FIELD_ELEMENTS_PER_EXT_BLOB,string"`
		KzgCommitmentInclusionProofDepth  uint64 `json:"KZG_COMMITMENT_INCLUSION_PROOF_DEPTH,string"`
		KzgCommitmentsInclusionProofDepth uint64 `json:"KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH,string"`
		MaxAttesterSlashingSize           uint64 `json:"MAX_ATTESTER_SLASHING_SIZE,string"`
		MaxSignedAggregateAndProofSize    uint64 `json:"MAX_SIGNED_AGGREGATE_AND_PROOF_SIZE,string"`
		MaxSignedExecutionPayloadBidSize  uint64 `json:"MAX_SIGNED_EXECUTION_PAYLOAD_BID_SIZE,string"`
		UpdateTimeout                     uint64 `json:"UPDATE_TIMEOUT,string"`
		SlotDurationMs                    uint64 `json:"SLOT_DURATION_MS,string"`
		MinEpochsForBlockRequests         uint64 `json:"MIN_EPOCHS_FOR_BLOCK_REQUESTS,string"`
	}{
		BeaconChainConfig:                 a.beaconChainCfg,
		NetworkConfig:                     a.netConfig,
		AttestationSubnetExtraBits:        clparams.AttestationSubnetExtraBits,
		EpochsPerSubnetSubscription:       clparams.EpochsPerSubnetSubscription,
		ReorgParentWeightThreshold:        clparams.ReorgParentWeightThreshold,
		ReorgMaxEpochsSinceFinalization:   clparams.ReorgMaxEpochsSinceFinalization,
		ProposerReorgCutoffBps:            clparams.ProposerReorgCutoffBps,
		ConfirmationByzantineThreshold:    clparams.ConfirmationByzantineThreshold,
		CellsPerExtBlob:                   params.CellsPerExtBlob,
		FieldElementsPerCell:              params.FieldElementsPerCell,
		FieldElementsPerExtBlob:           params.FieldElementsPerExtBlob,
		KzgCommitmentInclusionProofDepth:  cltypes.CommitmentBranchSize,
		KzgCommitmentsInclusionProofDepth: cltypes.KzgCommitmentsInclusionProofDepth,
		MaxAttesterSlashingSize:           gossip.MaxSSZSize(gossip.TopicNameAttesterSlashing, a.beaconChainCfg),
		MaxSignedAggregateAndProofSize:    gossip.MaxSSZSize(gossip.TopicNameBeaconAggregateAndProof, a.beaconChainCfg),
		MaxSignedExecutionPayloadBidSize:  uint64(maxSignedExecutionPayloadBidSSZSize()),
		UpdateTimeout:                     a.beaconChainCfg.SlotsPerEpoch * a.beaconChainCfg.EpochsPerSyncCommitteePeriod,
		SlotDurationMs:                    a.beaconChainCfg.SecondsPerSlot * 1000,
		MinEpochsForBlockRequests:         a.beaconChainCfg.MinEpochsForBlockRequests(),
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
