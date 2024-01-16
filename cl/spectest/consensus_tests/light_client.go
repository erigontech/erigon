package consensus_tests

import (
	"io/fs"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/spectest"
	"github.com/stretchr/testify/require"
)

type LcBranch struct {
	Branch []string `yaml:"branch"`
}

var LightClientBeaconBlockBodyExecutionMerkleProof = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var proof [][32]byte
	switch c.CaseName {
	case "execution_merkle_proof":
		beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig)
		require.NoError(t, spectest.ReadSsz(root, c.Version(), spectest.ObjectSSZ, beaconBody))
		proof, err = beaconBody.ExecutionPayloadMerkleProof()
		require.NoError(t, err)
	case "current_sync_committee_merkle_proof":
		state := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, spectest.ReadSsz(root, c.Version(), spectest.ObjectSSZ, state))
		proof, err = state.CurrentSyncCommitteeBranch()
		require.NoError(t, err)
	case "next_sync_committee_merkle_proof":
		state := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, spectest.ReadSsz(root, c.Version(), spectest.ObjectSSZ, state))
		proof, err = state.NextSyncCommitteeBranch()
		require.NoError(t, err)
	case "finality_root_merkle_proof":
		state := state.New(&clparams.MainnetBeaconConfig)
		require.NoError(t, spectest.ReadSsz(root, c.Version(), spectest.ObjectSSZ, state))

		proof, err = state.FinalityRootBranch()
		require.NoError(t, err)
	default:
		t.Skip("skipping: ", c.CaseName)
	}

	// read proof.yaml
	proofYaml := LcBranch{}
	err = spectest.ReadYml(root, "proof.yaml", &proofYaml)
	require.NoError(t, err)

	branch := make([][32]byte, len(proofYaml.Branch))
	for i, b := range proofYaml.Branch {
		branch[i] = libcommon.HexToHash(b)
	}

	require.Equal(t, branch, proof)
	return nil
})
