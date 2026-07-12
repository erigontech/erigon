package cltypes

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestExecutionRequests_EncodingSizeSSZ_MatchesEncoded(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig

	// Pre-Gloas: 3 dynamic fields (Deposits, Withdrawals, Consolidations).
	e := NewExecutionRequestsWithVersion(cfg, clparams.ElectraVersion)
	e.ensureLists()
	encoded, err := e.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, len(encoded), e.EncodingSizeSSZ(),
		"pre-Gloas: EncodingSizeSSZ must match actual encoded length")

	// Post-Gloas: 5 dynamic fields (+ BuilderDeposits, BuilderExits).
	e2 := NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	e2.ensureLists()
	encoded2, err := e2.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, len(encoded2), e2.EncodingSizeSSZ(),
		"post-Gloas: EncodingSizeSSZ must match actual encoded length")
}
