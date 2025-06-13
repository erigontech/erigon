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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/spectest"
	"github.com/stretchr/testify/require"
)

type LcBranch struct {
	Branch []string `yaml:"branch"`
}

var LightClientBeaconBlockBodyExecutionMerkleProof = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var proof [][32]byte
	switch c.CaseName {
	case "execution_merkle_proof":
		beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, c.Version())
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
		branch[i] = common.HexToHash(b)
	}

	require.Equal(t, branch, proof)
	return nil
})
