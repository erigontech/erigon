// Copyright 2024 The Erigon Authors
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

package consensus_tests

import (
	"io/fs"
	"math"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/spectest"
	"github.com/stretchr/testify/require"
)

type MPTBranch struct {
	Branch    []string `yaml:"branch"`
	Leaf      string   `yaml:"leaf"`
	LeafIndex uint64   `yaml:"leaf_index"`
}

var Eip4844MerkleProof = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	// read proof.yaml
	proofYaml := MPTBranch{}
	err = spectest.ReadYml(root, "proof.yaml", &proofYaml)
	require.NoError(t, err)

	// read branch from proof.yaml
	branch := make([][32]byte, len(proofYaml.Branch))
	for i, b := range proofYaml.Branch {
		branch[i] = common.HexToHash(b)
	}
	// get the depth of the merkle proof
	depth := uint64(math.Floor(math.Log2(float64(proofYaml.LeafIndex))))
	// get the leaf
	leaf := common.HexToHash(proofYaml.Leaf)
	beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, c.Version())
	require.NoError(t, spectest.ReadSsz(root, c.Version(), spectest.ObjectSSZ, beaconBody))

	singleProofBranch, err := beaconBody.KzgCommitmentMerkleProof(0)
	kzgCommitmentsProofBranch := singleProofBranch[len(singleProofBranch)-int(depth):] // only need to get the last depth elements
	require.NoError(t, err)

	// 1. check if the proof is the same as the branch
	require.Equal(t, branch, kzgCommitmentsProofBranch)

	bodyRoot, err := beaconBody.HashSSZ()
	require.NoError(t, err)
	proofHashes := make([]common.Hash, len(branch))
	for i := range branch {
		proofHashes[i] = common.Hash(branch[i])
	}
	// 2. check if the proof of entire kzg commitments is correct
	require.True(t, utils.IsValidMerkleBranch(leaf, proofHashes, depth, proofYaml.LeafIndex, bodyRoot))

	// 3. Then check each kzg commitment inclusion proof
	for i := 0; i < beaconBody.BlobKzgCommitments.Len(); i++ {
		proof, err := beaconBody.KzgCommitmentMerkleProof(i)
		require.NoError(t, err)
		commitmentInclusionProof := solid.NewHashVector(len(proof))
		for j := 0; j < len(proof); j++ {
			commitmentInclusionProof.Set(j, common.Hash(proof[j]))
		}
		require.True(t, cltypes.VerifyCommitmentInclusionProof(common.Bytes48(*beaconBody.BlobKzgCommitments.Get(i)), commitmentInclusionProof, uint64(i), c.Version(), bodyRoot))
	}
	return nil
})
