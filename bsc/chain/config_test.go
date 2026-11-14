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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
)

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
