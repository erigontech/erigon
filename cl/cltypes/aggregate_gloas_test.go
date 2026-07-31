package cltypes

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestAggregateAndProofHashDoesNotChangeAggregateVersion(t *testing.T) {
	aggregate := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0x03}, 2048),
		Data:            &solid.AttestationData{},
		CommitteeBits:   solid.NewBitVector(64),
	}
	aggregate.SetVersion(clparams.GloasVersion)
	want, err := aggregate.HashSSZ()
	require.NoError(t, err)

	_, err = (&AggregateAndProof{Aggregate: aggregate}).HashSSZ()
	require.NoError(t, err)
	got, err := aggregate.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)
}
