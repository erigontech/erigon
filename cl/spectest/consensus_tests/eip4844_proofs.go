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
	"testing"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
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

	branch := make([][32]byte, len(proofYaml.Branch))
	for i, b := range proofYaml.Branch {
		branch[i] = libcommon.HexToHash(b)
	}
	leaf := libcommon.HexToHash(proofYaml.Leaf)
	beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, spectest.ReadSsz(root, c.Version(), spectest.ObjectSSZ, beaconBody))
	proof, err := beaconBody.KzgCommitmentMerkleProof(0)
	require.NoError(t, err)

	require.Equal(t, branch, proof)
	bodyRoot, err := beaconBody.HashSSZ()
	require.NoError(t, err)
	proofHashes := make([]libcommon.Hash, len(proof))
	for i := range proof {
		proofHashes[i] = libcommon.Hash(proof[i])
	}
	require.True(t, utils.IsValidMerkleBranch(leaf, proofHashes, 17, proofYaml.LeafIndex, bodyRoot)) // Test if this is correct
	hashList := solid.NewHashVector(17)
	for i, h := range proof {
		hashList.Set(i, libcommon.Hash(h))
	}
	require.True(t, cltypes.VerifyCommitmentInclusionProof(libcommon.Bytes48(*beaconBody.BlobKzgCommitments.Get(0)), hashList, 0, c.Version(), bodyRoot))
	return nil

})
