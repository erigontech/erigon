package gossip

import (
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

func IsTopicBlobSidecar(d string) bool {
	return strings.Contains(d, "blob_sidecar_")
}

func IsTopicSyncCommittee(d string) bool {
	return strings.Contains(d, "sync_committee_") && !strings.Contains(d, TopicNameSyncCommitteeContributionAndProof)
}
func IsTopicBeaconAttestation(d string) bool {
	return strings.Contains(d, "beacon_attestation_")
}

func SubnetIdFromTopicBeaconAttestation(d string) (uint64, error) {
	if !IsTopicBeaconAttestation(d) {
		return 0, fmt.Errorf("not a beacon attestation topic")
	}
	var id uint64
	_, err := fmt.Sscanf(d, TopicNamePrefixBeaconAttestation, &id)
	return id, err
}
