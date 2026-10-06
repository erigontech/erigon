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
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
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

// The Chapel system-contract upgrade sets, keyed by activation block or time.
// The digest pins their content so moving where they are stored cannot change them.
func TestChapelSystemContractUpgrades(t *testing.T) {
	t.Parallel()

	blockAlloc := bscchain.Chapel.Config.Parlia.BlockAlloc
	keys := slices.Collect(maps.Keys(blockAlloc))
	slices.SortFunc(keys, func(a, b string) int {
		x, _ := strconv.ParseUint(a, 10, 64)
		y, _ := strconv.ParseUint(b, 10, 64)
		return cmp.Compare(x, y)
	})
	assert.Equal(t, []string{
		"1010000", "1014369", "5582500", "13837000", "19203503", "22800220", "23603940",
		"28196022", "29295050", "29861024", "1702972800", "1710136800", "1711342800",
		"1719986788", "1724116996", "1740452880", "1744097580", "1748243100", "1762741500",
	}, keys)

	encoded, err := json.Marshal(blockAlloc)
	require.NoError(t, err)
	assert.Equal(t, "c5defca32ed7f5279fce7278436cac133bd8dd9fa5a1fe8489b546a317f6bb9d", fmt.Sprintf("%x", sha256.Sum256(encoded)))
}
