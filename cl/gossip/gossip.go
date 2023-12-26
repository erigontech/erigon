package gossip

import "strings"

const (
	TopicNameBeaconBlock             = "beacon_block"
	TopicNameBeaconAggregateAndProof = "beacon_aggregate_and_proof"
	TopicNameVoluntaryExit           = "voluntary_exit"
	TopicNameProposerSlashing        = "proposer_slashing"
	TopicNameAttesterSlashing        = "attester_slashing"
	TopicNameBlsToExecutionChange    = "bls_to_execution_change"
)

func TopicNameBlobSidecar(d int) string {
	return "blob_sidecar_%d"
}

func IsTopicBlobSidecar(d string) bool {
	return strings.Contains(d, "blob_sidecar_")
}
