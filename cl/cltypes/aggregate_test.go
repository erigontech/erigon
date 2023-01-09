package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestAggregateMarshal(t *testing.T) {
	agg := &cltypes.SignedAggregateAndProof{
		Message: &cltypes.AggregateAndProof{
			AggregatorIndex: 11,
			Aggregate:       attestations[0],
		},
	}
	enc, err := agg.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, agg.SizeSSZ(), len(enc))
	agg2 := &cltypes.SignedAggregateAndProof{}
	require.NoError(t, agg2.UnmarshalSSZ(enc))
	require.Equal(t, agg, agg2)
}
