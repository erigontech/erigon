// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
