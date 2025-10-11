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

package gossip

import (
	"errors"
	"fmt"
	"strings"
)

const (
	TopicNameBeaconBlock                       = "beacon_block"
	TopicNameBeaconAggregateAndProof           = "beacon_aggregate_and_proof"
	TopicNameVoluntaryExit                     = "voluntary_exit"
	TopicNameProposerSlashing                  = "proposer_slashing"
	TopicNameAttesterSlashing                  = "attester_slashing"
	TopicNameBlsToExecutionChange              = "bls_to_execution_change"
	TopicNameSyncCommitteeContributionAndProof = "sync_committee_contribution_and_proof"

	TopicNameLightClientFinalityUpdate   = "light_client_finality_update"
	TopicNameLightClientOptimisticUpdate = "light_client_optimistic_update"

	TopicNamePrefixBlobSidecar       = "blob_sidecar_%d"
	TopicNamePrefixBeaconAttestation = "beacon_attestation_%d"
	TopicNamePrefixSyncCommittee     = "sync_committee_%d"
	TopicNamePrefixDataColumnSidecar = "data_column_sidecar_%d"
)

func TopicNameBlobSidecar(d uint64) string {
	return fmt.Sprintf(TopicNamePrefixBlobSidecar, d)
}

func TopicNameBeaconAttestation(d uint64) string {
	return fmt.Sprintf(TopicNamePrefixBeaconAttestation, d)
}

func TopicNameSyncCommittee(d int) string {
	return fmt.Sprintf(TopicNamePrefixSyncCommittee, d)
}

func TopicNameDataColumnSidecar(d uint64) string {
	return fmt.Sprintf(TopicNamePrefixDataColumnSidecar, d)
}

func IsTopicBlobSidecar(d string) bool {
	return strings.Contains(d, "blob_sidecar_")
}

func IsTopicDataColumnSidecar(d string) bool {
	return strings.Contains(d, "data_column_sidecar_")
}

func IsTopicSyncCommittee(d string) bool {
	return strings.Contains(d, "sync_committee_") && !strings.Contains(d, TopicNameSyncCommitteeContributionAndProof)
}
func IsTopicBeaconAttestation(d string) bool {
	return strings.Contains(d, "beacon_attestation_")
}

func SubnetIdFromTopicBeaconAttestation(d string) (uint64, error) {
	if !IsTopicBeaconAttestation(d) {
		return 0, errors.New("not a beacon attestation topic")
	}
	var id uint64
	_, err := fmt.Sscanf(d, TopicNamePrefixBeaconAttestation, &id)
	return id, err
}
