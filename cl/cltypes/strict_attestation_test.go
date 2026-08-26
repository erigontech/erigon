package cltypes

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestIndexedAttestationDecodeSSZStrictRejectsOffsetGap(t *testing.T) {
	attestation := NewIndexedAttestation(clparams.GloasVersion)
	attestation.AttestingIndices.Append(1)
	encoded, err := attestation.EncodeSSZ(nil)
	require.NoError(t, err)
	require.NoError(t, NewIndexedAttestation(clparams.GloasVersion).DecodeSSZStrict(encoded, int(clparams.GloasVersion)))

	const fixedSize = 228
	malformed := append([]byte(nil), encoded[:fixedSize]...)
	malformed = append(malformed, make([]byte, 4)...)
	malformed = append(malformed, encoded[fixedSize:]...)
	binary.LittleEndian.PutUint32(malformed, fixedSize+4)
	require.Error(t, NewIndexedAttestation(clparams.GloasVersion).DecodeSSZStrict(malformed, int(clparams.GloasVersion)))
}

func TestAttesterSlashingDecodeSSZStrictRejectsNestedIndexedGap(t *testing.T) {
	slashing := NewAttesterSlashing(clparams.GloasVersion)
	slashing.Attestation_1.AttestingIndices.Append(1)
	encoded, err := slashing.EncodeSSZ(nil)
	require.NoError(t, err)
	require.NoError(t, NewAttesterSlashing(clparams.GloasVersion).DecodeSSZStrict(encoded, int(clparams.GloasVersion)))

	const outerFixedSize = 8
	const indexedFixedSize = 228
	insertAt := outerFixedSize + indexedFixedSize
	malformed := append([]byte(nil), encoded[:insertAt]...)
	malformed = append(malformed, make([]byte, 4)...)
	malformed = append(malformed, encoded[insertAt:]...)
	binary.LittleEndian.PutUint32(malformed[4:], binary.LittleEndian.Uint32(encoded[4:])+4)
	binary.LittleEndian.PutUint32(malformed[outerFixedSize:], indexedFixedSize+4)
	require.Error(t, NewAttesterSlashing(clparams.GloasVersion).DecodeSSZStrict(malformed, int(clparams.GloasVersion)))
}
