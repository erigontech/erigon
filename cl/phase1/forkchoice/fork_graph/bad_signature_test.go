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

package fork_graph

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func newBlock1Graph(t *testing.T) (*forkGraphDisk, func() *cltypes.SignedBeaconBlock) {
	t.Helper()
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	decode := func() *cltypes.SignedBeaconBlock {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		require.NoError(t, utils.DecodeSSZSnappy(block, block1, int(clparams.Phase0Version)))
		return block
	}
	return graph.(*forkGraphDisk), decode
}

// The block root does not cover the signature, so a copy of a block with a bad proposer signature must not
// mark the root invalid: the copy with the right signature must import as usual.
func TestBadProposerSignatureDoesNotMarkTheRootInvalid(t *testing.T) {
	graph, decode := newBlock1Graph(t)
	valid, forged := decode(), decode()
	forged.Signature[5] ^= 0xff
	root, err := valid.Block.HashSSZ()
	require.NoError(t, err)

	_, status, err := graph.AddChainSegment(forged, true)
	require.Error(t, err)
	require.Equal(t, InvalidBlock, status)
	require.False(t, graph.IsBlockInvalid(common.Hash(root)))

	_, status, err = graph.AddChainSegment(valid, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)
	require.False(t, graph.IsBlockInvalid(common.Hash(root)))
}

// A proposer index outside the registry makes the block itself invalid, so its root stays marked.
func TestUnknownProposerMarksTheRootInvalid(t *testing.T) {
	graph, decode := newBlock1Graph(t)
	block := decode()
	block.Block.ProposerIndex = 1 << 40
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	_, status, err := graph.AddChainSegment(block, true)
	require.Error(t, err)
	require.Equal(t, InvalidBlock, status)
	require.True(t, graph.IsBlockInvalid(common.Hash(root)))
}
