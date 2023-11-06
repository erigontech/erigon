package utils_test

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

func TestSSZSnappy(t *testing.T) {
	verySussyMessage := &cltypes.Metadata{
		SeqNumber: 69, // :D
		Attnets:   96, // :(
	}
	sussyEncoded, err := utils.EncodeSSZSnappy(verySussyMessage)
	require.NoError(t, err)
	sussyDecoded := &cltypes.Metadata{}
	require.NoError(t, utils.DecodeSSZSnappy(sussyDecoded, sussyEncoded, 0))
	require.Equal(t, verySussyMessage.SeqNumber, sussyDecoded.SeqNumber)
	require.Equal(t, verySussyMessage.Attnets, sussyDecoded.Attnets)
}

func TestPlainSnappy(t *testing.T) {
	msg := common.Hex2Bytes("10103849358111387348383738784374783811111754097864786873478675489485765483936576486387645456876772090909090ff")
	sussyEncoded := utils.CompressSnappy(msg)
	sussyDecoded, err := utils.DecompressSnappy(sussyEncoded)
	require.NoError(t, err)
	require.Equal(t, msg, sussyDecoded)
}

func TestLiteralConverters(t *testing.T) {
	require.Equal(t, utils.Uint32ToBytes4(600), [4]byte{0x0, 0x0, 0x2, 0x58})
	require.Equal(t, utils.BytesToBytes4([]byte{10, 23, 56, 7, 8, 5}), [4]byte{10, 23, 56, 7})
	require.Equal(t, utils.Uint64ToLE(600), []byte{0x58, 0x2, 0x0, 0x0, 0x0, 0x0, 0x00, 0x00})
}
