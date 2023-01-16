package cltypes_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
)

var testEth1Data = &cltypes.Eth1Data{
	Root:         libcommon.HexToHash("0x2"),
	BlockHash:    libcommon.HexToHash("0x3"),
	DepositCount: 69,
}

var expectedTestEth1DataMarshalled = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000245000000000000000000000000000000000000000000000000000000000000000000000000000003")
var expectedTestEth1DataRoot = common.Hex2Bytes("adbafa10f1d6046b59cb720371c5e70ce2c6c3067b0e87985f5cd0899a515886")

func TestEth1DataMarshalUnmarmashal(t *testing.T) {
	marshalled, err := testEth1Data.MarshalSSZ()
	require.NoError(t, err)
	assert.Equal(t, marshalled, expectedTestEth1DataMarshalled)
	testData2 := &cltypes.Eth1Data{}
	require.NoError(t, testData2.UnmarshalSSZ(marshalled))
	require.Equal(t, testData2, testEth1Data)
}

func TestEth1DataHashTreeRoot(t *testing.T) {
	root, err := testEth1Data.HashTreeRoot()
	require.NoError(t, err)
	assert.Equal(t, root[:], expectedTestEth1DataRoot)
}
