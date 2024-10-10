package cltypes_test

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

var (
	serializedBlsToELSnappy = common.Hex2Bytes("ac01f0ab0bdacb3c8aece06797f49960af089e606d3f5ebaed8958ee04100851e410f3f575e9b4693e84556d48b57f9f5a160b9d111fe1922c13fb47ac76808f60cd2c3d84e64045c613a42ddf63a9025192753fe88988698039e5571078be15ec8a191f9fe75a66746bf147a470437fe3e5e2022afe697829040f9abf3e6b02b4fbb3016e864bac693563a9d52672dcb0e1f6e275a19d8b8104ebf76f108d13aa4b074e19b63ed3b2b73e9df58b7c7f")
	blsToElHash             = common.Hex2Bytes("48e53e48cd28246d85fd93ac0b01970edd2a75d8385605197c9e8fe1f415fa4e")
)

func TestBLSToEL(t *testing.T) {
	decompressed, _ := utils.DecompressSnappy(serializedBlsToELSnappy)
	obj := &cltypes.SignedBLSToExecutionChange{}
	require.NoError(t, obj.DecodeSSZ(decompressed, 1))
	root, err := obj.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root[:], blsToElHash)
	reencoded, err := obj.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, reencoded, decompressed)
}
