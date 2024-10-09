package state_accessors

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestSlotData(t *testing.T) {
	m := &SlotData{
		Version:                      clparams.CapellaVersion,
		Eth1Data:                     &cltypes.Eth1Data{},
		Eth1DepositIndex:             0,
		NextWithdrawalIndex:          0,
		NextWithdrawalValidatorIndex: 0,
		Fork:                         &cltypes.Fork{Epoch: 12},
	}
	var b bytes.Buffer
	if err := m.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	m2 := &SlotData{}
	if err := m2.ReadFrom(&b); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, m, m2)
}
