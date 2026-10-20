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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// EncodingSizeSSZ must grow by exactly the encoded delta when a field's
// content grows, for every field gated by a fork version.
func TestBlindedBeaconBodyEncodingSizeTracksEncoding(t *testing.T) {
	for _, v := range []clparams.StateVersion{clparams.DenebVersion, clparams.ElectraVersion} {
		t.Run(v.String(), func(t *testing.T) {
			body := NewBlindedBeaconBody(&clparams.MainnetBeaconConfig, v)
			require.Equal(t, v, body.Version, "constructor must honor the version argument")

			enc0, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			size0 := body.EncodingSizeSSZ()

			body.BlobKzgCommitments.Append(&KZGCommitment{})
			enc1, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			size1 := body.EncodingSizeSSZ()
			require.Equal(t, len(enc1)-len(enc0), size1-size0, "BlobKzgCommitments not reflected in EncodingSizeSSZ")

			body.ExecutionChanges.Append(&SignedBLSToExecutionChange{Message: &BLSToExecutionChange{}})
			enc2, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			size2 := body.EncodingSizeSSZ()
			require.Equal(t, len(enc2)-len(enc1), size2-size1, "ExecutionChanges over-counted in EncodingSizeSSZ")
		})
	}
}
