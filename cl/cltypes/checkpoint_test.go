package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCheckpoint = &cltypes.Checkpoint{
	Epoch: 69,
	Root:  common.HexToHash("0x3"),
}

var expectedTestCheckpointMarshalled = common.Hex2Bytes("45000000000000000000000000000000000000000000000000000000000000000000000000000003")
var expectedTestCheckpointRoot = common.Hex2Bytes("be8567f9fdae831b10720823dbcf0e3680e61d6a2a27d85ca00f6c15a7bbb1ea")

func TestCheckpointMarshalUnmarmashal(t *testing.T) {
	marshalled, err := testCheckpoint.MarshalSSZ()
	require.NoError(t, err)
	assert.Equal(t, marshalled, expectedTestCheckpointMarshalled)
	checkpoint := &cltypes.Checkpoint{}
	require.NoError(t, checkpoint.UnmarshalSSZ(marshalled))
	require.Equal(t, checkpoint, testCheckpoint)
}

func TestCheckpointHashTreeRoot(t *testing.T) {
	root, err := testCheckpoint.HashTreeRoot()
	require.NoError(t, err)
	assert.Equal(t, root[:], expectedTestCheckpointRoot)
}
