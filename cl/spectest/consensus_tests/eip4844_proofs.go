package consensus_tests

import (
	"io/fs"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/spectest"
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
	beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig)
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
