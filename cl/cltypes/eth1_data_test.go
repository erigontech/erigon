package cltypes_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

var testEth1Data = &cltypes.Eth1Data{
	Root:         libcommon.HexToHash("0x2"),
	BlockHash:    libcommon.HexToHash("0x3"),
	DepositCount: 69,
}

var expectedTestEth1DataMarshalled = libcommon.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000245000000000000000000000000000000000000000000000000000000000000000000000000000003")
var expectedTestEth1DataRoot = libcommon.Hex2Bytes("adbafa10f1d6046b59cb720371c5e70ce2c6c3067b0e87985f5cd0899a515886")

func TestEth1DataMarshalUnmarmashal(t *testing.T) {
	marshalled, _ := testEth1Data.EncodeSSZ(nil)
	assert.Equal(t, marshalled, expectedTestEth1DataMarshalled)
	testData2 := &cltypes.Eth1Data{}
	require.NoError(t, testData2.DecodeSSZ(marshalled, 0))
	require.Equal(t, testData2, testEth1Data)
}

func TestEth1DataHashTreeRoot(t *testing.T) {
	root, err := testEth1Data.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, root[:], expectedTestEth1DataRoot)
}
