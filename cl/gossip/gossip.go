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

func TopicNameBlobSidecar(d int) string {
	return fmt.Sprintf(TopicNamePrefixBlobSidecar, d)
}

func IsTopicBlobSidecar(d string) bool {
	return strings.Contains(d, "blob_sidecar_")
}
