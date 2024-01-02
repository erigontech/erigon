package state_accessors

import (
	"bytes"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestEpochData(t *testing.T) {
	e := &EpochData{
		TotalActiveBalance:          123,
		JustificationBits:           &cltypes.JustificationBits{true},
		Fork:                        &cltypes.Fork{},
		CurrentJustifiedCheckpoint:  solid.NewCheckpointFromParameters(libcommon.Hash{}, 123),
		PreviousJustifiedCheckpoint: solid.NewCheckpointFromParameters(libcommon.Hash{}, 123),
		FinalizedCheckpoint:         solid.NewCheckpointFromParameters(libcommon.Hash{}, 123),
		HistoricalSummariesLength:   235,
		HistoricalRootsLength:       345,
	}
	var b bytes.Buffer
	if err := e.WriteTo(&b); err != nil {
		t.Fatal(err)
	}

	e2 := &EpochData{}
	if err := e2.ReadFrom(&b); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, e, e2)
}
