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

package blob_storage

import (
	"sync"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

type unusedBlobStorage struct{ BlobStorage }

type firstInsertError struct{}

func (firstInsertError) Error() string { return "first" }

type secondInsertError struct{}

func (secondInsertError) Error() string { return "second" }

func makeInsertSidecar(t *testing.T, slot uint64) (common.Hash, *cltypes.BlobSidecar) {
	t.Helper()
	blob := cltypes.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&blob), commitment, 0)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	blockCommitment := cltypes.KZGCommitment(commitment)
	block.Block.Body.BlobKzgCommitments.Append(&blockCommitment)
	branch, err := block.Block.Body.KzgCommitmentMerkleProof(0)
	require.NoError(t, err)
	inclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
	for index, hash := range branch {
		inclusionProof.Set(index, hash)
	}
	header := block.SignedBeaconBlockHeader()
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)
	return root, cltypes.NewBlobSidecar(0, &blob, common.Bytes48(commitment), common.Bytes48(proof), header, inclusionProof)
}

func TestConcurrentBlobGroupErrorsAcceptDifferentConcreteTypes(t *testing.T) {
	rootA, sidecarA := makeInsertSidecar(t, 1)
	rootB, sidecarB := makeInsertSidecar(t, 2)
	identifiers := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	identifiers.Append(&cltypes.BlobIdentifier{BlockRoot: rootA, Index: 0})
	identifiers.Append(&cltypes.BlobIdentifier{BlockRoot: rootB, Index: 0})
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once

	_, _, err := VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(t.Context(), unusedBlobStorage{}, identifiers, []*cltypes.BlobSidecar{sidecarA, sidecarB}, func(header *cltypes.SignedBeaconBlockHeader) error {
		ready <- struct{}{}
		if len(ready) == 2 {
			releaseOnce.Do(func() { close(release) })
		}
		<-release
		if header.Header.Slot == 1 {
			return firstInsertError{}
		}
		return secondInsertError{}
	})
	require.Error(t, err)
}
