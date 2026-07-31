package cltypes

import (
	"encoding/json"
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

func TestAggregateAndProofJSONPreservesGloasVersion(t *testing.T) {
	aggregate := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0x03}, 2048),
		Data:            &solid.AttestationData{},
		CommitteeBits:   solid.NewBitVector(64),
	}
	message := &AggregateAndProof{Aggregate: aggregate}
	message.SetVersion(clparams.GloasVersion)
	signed := &SignedAggregateAndProof{Message: message}
	signed.SetVersion(clparams.GloasVersion)

	for name, original := range map[string]interface {
		HashSSZ() ([32]byte, error)
	}{"message": message, "signed": signed} {
		t.Run(name, func(t *testing.T) {
			want, err := original.HashSSZ()
			require.NoError(t, err)
			encoded, err := json.Marshal(original)
			require.NoError(t, err)

			var decoded interface {
				HashSSZ() ([32]byte, error)
			}
			if name == "message" {
				value := &AggregateAndProof{}
				value.SetVersion(clparams.GloasVersion)
				decoded = value
			} else {
				value := &SignedAggregateAndProof{}
				value.SetVersion(clparams.GloasVersion)
				decoded = value
			}
			require.NoError(t, json.Unmarshal(encoded, decoded))
			got, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestAggregateAndProofJSONDoesNotCreateMissingNestedObjects(t *testing.T) {
	for _, input := range []string{`{}`, `{"aggregate":null}`} {
		message := &AggregateAndProof{}
		message.SetVersion(clparams.GloasVersion)
		require.NoError(t, json.Unmarshal([]byte(input), message))
		require.Nil(t, message.Aggregate)
	}

	for _, input := range []string{`{}`, `{"message":null}`} {
		signed := &SignedAggregateAndProof{}
		signed.SetVersion(clparams.GloasVersion)
		require.NoError(t, json.Unmarshal([]byte(input), signed))
		require.Nil(t, signed.Message)
	}
}
