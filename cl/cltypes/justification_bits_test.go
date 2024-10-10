package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestParticipationBits(t *testing.T) {
	bits := cltypes.JustificationBits{}
	bits.FromByte(2)
	require.Equal(t, bits, cltypes.JustificationBits{false, true, false, false})
	require.Equal(t, bits.Byte(), byte(2))
}
