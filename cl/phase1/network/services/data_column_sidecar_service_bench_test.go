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

package services

import (
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

// BenchmarkGloasDeferredVerificationPass measures one pass worth of deferred KZG verifications: a full verification
// budget on a column with the largest blob count of the mainnet config.
func BenchmarkGloasDeferredVerificationPass(b *testing.B) {
	blobCount := int(clparams.MainnetBeaconConfig.MaxBlobsPerBlockUpperBound())
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](blobCount, 48)
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	for row := range blobCount {
		var blob goethkzg.Blob
		for j := 0; j < len(blob); j += goethkzg.SerializedScalarSize {
			blob[j+31], blob[j+30] = byte(row+1), byte(j/goethkzg.SerializedScalarSize+row)
		}
		commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
		require.NoError(b, err)
		cells, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
		require.NoError(b, err)
		kzgCommitment := cltypes.KZGCommitment(commitment)
		commitments.Append(&kzgCommitment)
		sidecar.Column.Append(&cells[0])
		sidecar.KzgProofs.Append(&proofs[0])
	}
	require.True(b, das.VerifyDataColumnSidecarKZGProofsWithCommitments(sidecar, commitments))

	for b.Loop() {
		for range maxPendingGloasVerificationsPerPass {
			das.VerifyDataColumnSidecarKZGProofsWithCommitments(sidecar, commitments)
		}
	}
}
