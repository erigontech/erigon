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
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
)

// A Caplin runtime restart stops the Sentinel but not the p2p stack: the
// libp2p host, its peers and the discv5 table belong to the p2p manager and
// take an hour to rebuild, against a restart of seconds.
func TestSentinel_StopLeavesTheBorrowedHostConnectable(t *testing.T) {
	ctrl := gomock.NewController(t)
	stateReader := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	stateReader.EXPECT().GetAdvertisedCgc().Return(uint64(8)).AnyTimes()

	sent, pm, _ := newDetachedTestSentinel(t, stateReader)

	peerHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { peerHost.Close() })

	sent.Stop()

	require.NoError(t, pm.Host().Connect(t.Context(),
		peer.AddrInfo{ID: peerHost.ID(), Addrs: peerHost.Addrs()}),
		"the host belongs to the p2p manager, so it must still reach peers after the sentinel over it stops")
}
