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

package chain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bscchain "github.com/erigontech/erigon/bsc/chain"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/p2p/enode"
)

// TestChapelForkPrecompiles pins that Chapel's fork schedule actually reaches the
// EVM. A fork block parsed into the config but never gated resolves to the previous
// fork's precompile set, which under-charges gas and diverges the state root.
func TestChapelForkPrecompiles(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)

	blsVerify := accounts.InternAddress(common.BytesToAddress([]byte{102}))

	for _, tc := range []struct {
		name  string
		block uint64
		bls   bool
	}{
		{"planck", 28196022, false},
		{"luban", 29613785, true},
		{"plato", 29861024, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockContext := evmtypes.BlockContext{BlockNumber: tc.block}
			set := vm.Precompiles(blockContext.Rules(spec.Config))
			if tc.bls {
				require.Contains(t, set, blsVerify)
				return
			}
			require.NotContains(t, set, blsVerify)
		})
	}
}

func TestChapelSpec(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	require.False(t, spec.IsEmpty())

	assert.Equal(t, uint64(97), spec.Config.ChainID.Uint64())
	assert.Equal(t, chain.ParliaRules, spec.Config.Rules)
	assert.Equal(t, bscchain.Chapel.GenesisHash, spec.GenesisHash)
}

// TestChapelStaticPeers covers the path setStaticPeers takes when --staticpeers
// is not given: Chapel publishes no bootnodes of its own, so losing this lookup
// would leave the node with nothing to dial.
func TestChapelStaticPeers(t *testing.T) {
	t.Parallel()

	peers := chainspec.StaticPeerURLsOfChain(networkname.Chapel)
	require.Len(t, peers, 4)
	assert.Equal(t, bscchain.Chapel.StaticPeers, peers)

	nodes, err := enode.ParseNodesFromURLs(peers)
	require.NoError(t, err)
	require.Len(t, nodes, 4)
	for _, n := range nodes {
		assert.Equal(t, 30311, n.TCP())
	}
}
