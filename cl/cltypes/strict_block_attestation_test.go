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
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/ssz"
)

func TestSignedBeaconBlockDecodeSSZStrictRejectsNestedAttestationOffsetPastBuffer(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	attestation := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxCommitteesPerSlot*cfg.MaxValidatorsPerCommittee)),
		Data:            &solid.AttestationData{},
		CommitteeBits:   solid.NewBitVector(int(cfg.MaxCommitteesPerSlot)),
	}
	attestation.Signature[0] = 0xa5
	attestationEncoded, err := attestation.EncodeSSZ(nil)
	require.NoError(t, err)

	block := NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Body.Attestations.Append(attestation)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	attestationOffset := bytes.Index(encoded, attestationEncoded)
	require.NotEqual(t, -1, attestationOffset)
	binary.LittleEndian.PutUint32(encoded[attestationOffset:attestationOffset+4], 1<<20)

	decoded := NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	err = decoded.DecodeSSZStrict(encoded, int(clparams.GloasVersion))
	require.True(t, errors.Is(err, ssz.ErrBadOffset), err)
}
