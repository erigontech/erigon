package gossip

import (
	"strconv"
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

	TopicNameContributionAndProof        = "contribution_and_proof"
	TopicNameLightClientFinalityUpdate   = "light_client_finality_update"
	TopicNameLightClientOptimisticUpdate = "light_client_optimistic_update"

	TopicNamePrefixBlobSidecar = "blob_sidecar_"
)

func TopicNameBlobSidecar(d int) string {
	return TopicNamePrefixBlobSidecar + strconv.Itoa(d)
}

func IsTopicBlobSidecar(d string) bool {
	return strings.Contains(d, TopicNamePrefixBlobSidecar)
}
