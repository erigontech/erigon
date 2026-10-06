package gossip

import (
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
)

const (
	sszOffsetSize            = 4
	blsSignatureSize         = 96
	attestationDataSize      = 128
	beaconBlockHeaderSize    = 3*32 + 2*8
	voluntaryExitSize        = 2 * 8
	blsToExecutionChangeSize = 8 + 48 + 20
	singleAttestationSize    = 2*8 + attestationDataSize + blsSignatureSize
	syncCommitteeMessageSize = 8 + 32 + 8 + blsSignatureSize
)

// ExtractTopicName returns the name segment of a "/eth2/<fork_digest>/<name>/<codec>" topic, or "" for any other shape.
func ExtractTopicName(topic string) string {
	tokens := strings.Split(topic, "/")
	if len(tokens) != 5 {
		return ""
	}
	return tokens[3]
}

// MaxUncompressedSize returns the largest SSZ encoding a message on topicName can have,
// which the spec requires peers to enforce next to the network payload limit. Types
// without a cheap bound fall back to that limit.
func MaxUncompressedSize(topicName string, beacon *clparams.BeaconChainConfig, network *clparams.NetworkConfig) uint64 {
	committees, validators := beacon.MaxCommitteesPerSlot, beacon.MaxValidatorsPerCommittee
	switch {
	case topicName == TopicNameBeaconAggregateAndProof:
		aggregateAndProof := sszOffsetSize + 8 + blsSignatureSize + electraAttestationSize(committees, validators)
		return sszOffsetSize + blsSignatureSize + aggregateAndProof
	case IsTopicBeaconAttestation(topicName):
		return max(denebAttestationSize(validators), singleAttestationSize)
	case topicName == TopicNameAttesterSlashing:
		indexedAttestation := sszOffsetSize + attestationDataSize + blsSignatureSize + 8*committees*validators
		return 2*sszOffsetSize + 2*indexedAttestation
	case topicName == TopicNameProposerSlashing:
		return 2 * (beaconBlockHeaderSize + blsSignatureSize)
	case topicName == TopicNameVoluntaryExit:
		return voluntaryExitSize + blsSignatureSize
	case topicName == TopicNameBlsToExecutionChange:
		return blsToExecutionChangeSize + blsSignatureSize
	case IsTopicSyncCommittee(topicName):
		return syncCommitteeMessageSize
	case topicName == TopicNameSyncCommitteeContributionAndProof:
		contribution := 8 + 32 + 8 + uint64(beacon.SyncCommitteeAggregationBitsSize()) + blsSignatureSize
		return blsSignatureSize + 8 + blsSignatureSize + contribution
	default:
		return network.GossipMaxSize
	}
}

func electraAttestationSize(committees, validators uint64) uint64 {
	return sszOffsetSize + attestationDataSize + blsSignatureSize + (committees+7)/8 + bitlistSize(committees*validators)
}

func denebAttestationSize(validators uint64) uint64 {
	return sszOffsetSize + attestationDataSize + blsSignatureSize + bitlistSize(validators)
}

// bitlistSize includes the delimiter bit SSZ appends to every bitlist.
func bitlistSize(bits uint64) uint64 {
	return bits/8 + 1
}
