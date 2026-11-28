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

package raw

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
)

func newHezeTestState(t *testing.T) *BeaconState {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	state := New(&cfg)
	state.version = clparams.HezeVersion
	state.slot = 33
	state.latestExecutionPayloadHeader = cltypes.NewEth1Header(clparams.HezeVersion)
	state.latestExecutionPayloadBid = &cltypes.ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		InclusionListBits:  solid.NewBitVector(clparams.InclusionListCommitteeSize),
	}
	require.NoError(t, state.latestExecutionPayloadBid.InclusionListBits.SetBitAt(2, true))
	return state
}

func hezeStateActiveFieldsForTest() []bool {
	active := make([]bool, StateLeafSizeGloas)
	for i := range active {
		active[i] = true
	}
	for _, gap := range []StateLeafIndex{Eth1DataLeafIndex, Eth1DataVotesLeafIndex, Eth1DepositIndexLeafIndex, DepositRequestsStartIndexLeafIndex} {
		active[gap] = false
	}
	return active
}

func TestHezeStateHashSkipsRemovedFields(t *testing.T) {
	state := newHezeTestState(t)
	root, err := state.HashSSZ()
	require.NoError(t, err)

	var schema []any
	for i, active := range hezeStateActiveFieldsForTest() {
		if active {
			schema = append(schema, state.leaves[i*32:(i+1)*32])
		}
	}
	expected, err := merkle_tree.ProgressiveContainerRoot(hezeStateActiveFieldsForTest(), schema...)
	require.NoError(t, err)
	require.Equal(t, expected, root)

	state.eth1DepositIndex = 5
	state.depositRequestsStartIndex = 6
	state.markLeaf(Eth1DepositIndexLeafIndex, DepositRequestsStartIndexLeafIndex)
	changed, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, changed)
}

func TestHezeStateSSZRoundTrip(t *testing.T) {
	gloas := newHezeTestState(t)
	gloas.version = clparams.GloasVersion
	gloas.latestExecutionPayloadBid.InclusionListBits = nil
	gloasEncoded, err := gloas.EncodeSSZ(nil)
	require.NoError(t, err)

	state := newHezeTestState(t)
	state.eth1DepositIndex = 5
	encoded, err := state.EncodeSSZ(nil)
	require.NoError(t, err)
	// eth1_data, the eth1_data_votes offset, eth1_deposit_index and deposit_requests_start_index are gone;
	// inclusion_list_bits adds 2 bytes.
	require.Equal(t, len(gloasEncoded)-72-4-8-8+2, len(encoded))
	require.Equal(t, len(encoded), state.EncodingSizeSSZ())

	decoded := New(state.beaconConfig)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.HezeVersion)))
	require.Equal(t, uint64(33), decoded.slot)
	require.True(t, decoded.latestExecutionPayloadBid.InclusionListBits.GetBitAt(2))

	want, err := state.HashSSZ()
	require.NoError(t, err)
	got, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)

	reencoded, err := decoded.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, encoded, reencoded)
}

func TestHezeStateCopyKeepsInclusionListBits(t *testing.T) {
	state := newHezeTestState(t)
	copied, err := state.Copy()
	require.NoError(t, err)
	require.True(t, copied.latestExecutionPayloadBid.InclusionListBits.GetBitAt(2))

	want, err := state.HashSSZ()
	require.NoError(t, err)
	got, err := copied.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestHezeStateJSONOmitsRemovedFields(t *testing.T) {
	state := newHezeTestState(t)
	out, err := json.Marshal(state)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &fields))
	for _, removed := range []string{"eth1_data", "eth1_data_votes", "eth1_deposit_index", "deposit_requests_start_index"} {
		require.NotContains(t, fields, removed)
	}
	require.Contains(t, fields, "latest_execution_payload_bid")
}

func TestHezeStateProofsUseActiveFieldsRoot(t *testing.T) {
	state := newHezeTestState(t)
	var wantActiveRoot common.Hash
	for i, active := range hezeStateActiveFieldsForTest() {
		if active {
			wantActiveRoot[i/8] |= 1 << uint(i%8)
		}
	}
	for name, branch := range map[string]func() ([][32]byte, error){
		"current sync committee": state.CurrentSyncCommitteeBranch,
		"next sync committee":    state.NextSyncCommitteeBranch,
		"finality root":          state.FinalityRootBranch,
	} {
		proof, err := branch()
		require.NoError(t, err, name)
		require.Equal(t, wantActiveRoot, common.Hash(proof[len(proof)-1]), name)
	}
}
