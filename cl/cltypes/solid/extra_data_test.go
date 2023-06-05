package solid_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestExtraData(t *testing.T) {
	// Create a new ExtraData instance
	extraData := solid.NewExtraData()

	// Set the bytes
	bytes := []byte{0x01, 0x02, 0x03}
	extraData.SetBytes(bytes)

	// Encode and decode the ExtraData
	encoded, err := extraData.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded := solid.NewExtraData()
	err = decoded.DecodeSSZ(encoded, 0)
	require.NoError(t, err)

	// Verify the bytes
	require.Equal(t, bytes, decoded.Bytes())

	// Verify the encoding size
	require.Equal(t, len(bytes), extraData.EncodingSizeSSZ())

	// Calculate the expected hash
	expectedHash, err := extraData.HashSSZ()
	require.NoError(t, err)

	// Verify the hash
	actualHash, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedHash, actualHash)
}
