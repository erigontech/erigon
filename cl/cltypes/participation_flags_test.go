package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestParticipationFlags(t *testing.T) {
	flagsList := cltypes.ParticipationFlagsListFromBytes([]byte{0, 0, 0, 0})
	flagsList[0] = flagsList[0].Add(4) // Turn on fourth bit
	require.True(t, flagsList[0].HasFlag(4))
}
