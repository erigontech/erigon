package gossip_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestExtractTopicName(t *testing.T) {
	require.Equal(t, "beacon_block", gossip.ExtractTopicName("/eth2/6c7a2141/beacon_block/ssz_snappy"))
	require.Equal(t, "", gossip.ExtractTopicName("/eth2/6c7a2141/beacon_block"))
	require.Equal(t, "", gossip.ExtractTopicName(""))
}

// Pins the hand-derived bounds to the largest encodings Erigon's own types report.
func TestMaxUncompressedSizeMatchesLargestEncoding(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	net := clparams.NetworkConfigs[chainspec.MainnetChainID]
	committees, validators := int(cfg.MaxCommitteesPerSlot), int(cfg.MaxValidatorsPerCommittee)

	fullBitlist := func(bits int) *solid.BitList {
		return solid.BitlistFromBytes(make([]byte, bits/8+1), bits)
	}
	fullIndexed := func() *cltypes.IndexedAttestation {
		n := committees * validators
		return &cltypes.IndexedAttestation{AttestingIndices: solid.NewRawUint64List(n, make([]uint64, n))}
	}
	electraAttestation := &solid.Attestation{
		AggregationBits: fullBitlist(committees * validators),
		Data:            &solid.AttestationData{},
		CommitteeBits:   solid.NewBitVector(committees),
	}
	denebAttestation := &solid.Attestation{AggregationBits: fullBitlist(validators), Data: &solid.AttestationData{}}

	largest := map[string]int{
		gossip.TopicNameBeaconAggregateAndProof: (&cltypes.SignedAggregateAndProof{
			Message: &cltypes.AggregateAndProof{Aggregate: electraAttestation},
		}).EncodingSizeSSZ(),
		gossip.TopicNameBeaconAttestation(3): max(denebAttestation.EncodingSizeSSZ(), (&solid.SingleAttestation{}).EncodingSizeSSZ()),
		gossip.TopicNameAttesterSlashing: (&cltypes.AttesterSlashing{
			Attestation_1: fullIndexed(), Attestation_2: fullIndexed(),
		}).EncodingSizeSSZ(),
		gossip.TopicNameProposerSlashing: (&cltypes.ProposerSlashing{
			Header1: &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}},
		}).EncodingSizeSSZ(),
		gossip.TopicNameVoluntaryExit:        (&cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{}}).EncodingSizeSSZ(),
		gossip.TopicNameBlsToExecutionChange: (&cltypes.SignedBLSToExecutionChange{Message: &cltypes.BLSToExecutionChange{}}).EncodingSizeSSZ(),
		gossip.TopicNameSyncCommittee(1):     (&cltypes.SyncCommitteeMessage{}).EncodingSizeSSZ(),
		gossip.TopicNameSyncCommitteeContributionAndProof: (&cltypes.SignedContributionAndProof{
			Message: &cltypes.ContributionAndProof{Contribution: &cltypes.Contribution{}},
		}).EncodingSizeSSZ(),
	}
	for topic, want := range largest {
		require.Equal(t, uint64(want), gossip.MaxUncompressedSize(topic, cfg, &net), topic)
	}
	require.Equal(t, uint64(16829), gossip.MaxUncompressedSize(gossip.TopicNameBeaconAggregateAndProof, cfg, &net))

	for _, topic := range []string{gossip.TopicNameBeaconBlock, gossip.TopicNameExecutionPayload, gossip.TopicNameDataColumnSidecar(0), ""} {
		require.Equal(t, net.GossipMaxSize, gossip.MaxUncompressedSize(topic, cfg, &net), topic)
	}
}
