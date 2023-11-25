package state_accessors

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestMinimalState(t *testing.T) {
	m := &MinimalBeaconState{
		Version:                      clparams.CapellaVersion,
		Eth1Data:                     &cltypes.Eth1Data{},
		Eth1DepositIndex:             0,
		JustificationBits:            &cltypes.JustificationBits{},
		NextWithdrawalIndex:          0,
		NextWithdrawalValidatorIndex: 0,
		LatestExecutionPayloadHeader: cltypes.NewEth1Header(clparams.CapellaVersion),
	}
	var b bytes.Buffer
	if err := m.Serialize(&b); err != nil {
		t.Fatal(err)
	}
	m2 := &MinimalBeaconState{}
	if err := m2.Deserialize(&b); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, m, m2)
}
