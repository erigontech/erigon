package cltypes_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"

	_ "embed"
)

var testAttData = &cltypes.AttestationData{
	Slot:            69,
	Index:           402,
	BeaconBlockHash: libcommon.HexToHash("123"),
	Source:          testCheckpoint,
	Target:          testCheckpoint,
}

var attestations = []*cltypes.Attestation{
	{
		AggregationBits: []byte{2},
		Data:            testAttData,
	},
	{
		AggregationBits: []byte{2},
		Data:            testAttData,
	},
	{
		AggregationBits: []byte{2},
		Data:            testAttData,
	},
	{
		AggregationBits: []byte{2},
		Data:            testAttData,
	},
}

var expectedAttestationMarshalled = "e4000000450000000000000092010000000000000000000000000000000000000000000000000000000000000000000000000123450000000000000000000000000000000000000000000000000000000000000000000000000000034500000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002"

func TestAttestationHashTest(t *testing.T) {
	hash, err := attestations[0].HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Bytes2Hex(hash[:]), "c9cf21a5c4273a2b85a84b5eff0e500dbafc8b20ecd21c59a87c610791112ba7")
}

func TestEncodeForStorage(t *testing.T) {
	enc := cltypes.EncodeAttestationsForStorage(attestations)
	require.Less(t, len(enc), attestations[0].EncodingSizeSSZ()*len(attestations))
	decAttestations, err := cltypes.DecodeAttestationsForStorage(enc)
	require.NoError(t, err)
	require.Equal(t, attestations, decAttestations)
}

func TestAttestationMarshalUnmarmashal(t *testing.T) {
	marshalled, err := attestations[0].EncodeSSZ(nil)
	require.NoError(t, err)
	assert.Equal(t, common.Bytes2Hex(marshalled[:]), expectedAttestationMarshalled)
	testData2 := &cltypes.Attestation{}
	require.NoError(t, testData2.DecodeSSZ(marshalled))
	require.Equal(t, testData2, attestations[0])
}

//go:embed tests/pending_attestation.ssz_snappy
var pendingAttestationTest []byte

func TestPendingAttestation(t *testing.T) {
	att := &cltypes.PendingAttestation{}
	encodedExpected, err := utils.DecompressSnappy(pendingAttestationTest)
	require.NoError(t, err)

	require.NoError(t, att.DecodeSSZ(encodedExpected))

	root, err := att.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.HexToHash("6d73ce691559544e2d4ec0071abd7e7a4dbe3a3ede4d7008e0f6db75deb40bde"), libcommon.Hash(root))

	encodedHave, err := att.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, encodedExpected, encodedHave)
}
