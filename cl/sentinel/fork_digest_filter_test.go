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

package sentinel

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func discoveredNode(t *testing.T, eth2 []byte) *enode.Node {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	var r enr.Record
	if eth2 != nil {
		r.Set(enr.WithEntry(clparams.NetworkConfigs[chainspec.MainnetChainID].Eth2key, eth2))
	}
	require.NoError(t, enode.SignV4(&r, key))
	node, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return node
}

// forkFilterSentinel is a sentinel whose current fork digest is 01020304.
func forkFilterSentinel(t *testing.T) *Sentinel {
	t.Helper()
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{1, 2, 3, 4}, nil).AnyTimes()
	cfg := &SentinelConfig{}
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	cfg.NetworkConfig = &networkConfig
	return &Sentinel{ethClock: clock, cfg: cfg}
}

// Discovery is shared by every network; a node that does not advertise our fork digest fails the status handshake.
func TestOnOurForkMatchesTheEnrForkDigest(t *testing.T) {
	s := forkFilterSentinel(t)

	forkID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 1} // digest, next fork version, next fork epoch
	require.True(t, s.onOurFork(discoveredNode(t, forkID)))
	other := append([]byte{1, 2, 3, 5}, forkID[4:]...)
	require.False(t, s.onOurFork(discoveredNode(t, other)), "another network or fork")
	require.False(t, s.onOurFork(discoveredNode(t, nil)), "no eth2 entry, e.g. an execution layer node")
	require.False(t, s.onOurFork(discoveredNode(t, forkID[:3])), "truncated eth2 entry")
}

// Without our digest the status handshake fails every peer, so nothing is worth dialling.
func TestOnOurForkRejectsNodesWithoutOurDigest(t *testing.T) {
	s := forkFilterSentinel(t)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{}, errors.New("no digest")).AnyTimes()
	s.ethClock = clock

	require.False(t, s.onOurFork(discoveredNode(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 1})))
}

// Both dial paths take discovered nodes through nodesOnOurFork; it passes on only the nodes of our fork, in order.
func TestNodesOnOurForkYieldsOnlyOurFork(t *testing.T) {
	s := forkFilterSentinel(t)
	forkID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 1}
	first, second := discoveredNode(t, forkID), discoveredNode(t, forkID)
	nodes := []*enode.Node{discoveredNode(t, nil), first, discoveredNode(t, append([]byte{9, 9, 9, 9}, forkID[4:]...)), second}

	it := s.nodesOnOurFork(enode.IterNodes(nodes))
	defer it.Close()
	var got []*enode.Node
	for it.Next() {
		got = append(got, it.Node())
	}
	require.Len(t, got, 2)
	require.Equal(t, []enode.ID{first.ID(), second.ID()}, []enode.ID{got[0].ID(), got[1].ID()})
}
