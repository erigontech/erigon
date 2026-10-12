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

package p2p

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

// The tests use a small peer pool, where the /16 cap is still larger than the /24 cap, so that
// they stay far below the system-wide connection limit.
const testMaxPeerCount = 64

type connOpener struct {
	t      *testing.T
	rm     network.ResourceManager
	scopes []network.ConnManagementScope
}

func newConnOpener(t *testing.T) *connOpener {
	rm, err := newResourceManager(testMaxPeerCount)
	require.NoError(t, err)
	o := &connOpener{t: t, rm: rm}
	t.Cleanup(func() {
		o.closeAll()
		require.NoError(t, rm.Close())
	})
	return o
}

// open opens an inbound connection scope from addr ("/ip4/..." or "/ip6/...").
func (o *connOpener) open(addr string) error {
	scope, err := o.rm.OpenConnection(network.DirInbound, false, multiaddr.StringCast(addr+"/tcp/30303"))
	if err == nil {
		o.scopes = append(o.scopes, scope)
	}
	return err
}

func (o *connOpener) closeAll() {
	for _, s := range o.scopes {
		s.Done()
	}
	o.scopes = nil
}

const errPerSubnetLimit = "connections per ip limit exceeded"

func TestSubnetConnLimitsScaleWithPeerCount(t *testing.T) {
	for _, tt := range []struct {
		maxPeerCount           uint64
		perBlock24, perBlock16 int
	}{
		{maxPeerCount: 0, perBlock24: 16, perBlock16: 16},
		{maxPeerCount: 32, perBlock24: 16, perBlock16: 16},
		{maxPeerCount: 64, perBlock24: 16, perBlock16: 32},
		{maxPeerCount: 128, perBlock24: 16, perBlock16: 64},
		{maxPeerCount: 512, perBlock24: 64, perBlock16: 256},
	} {
		perBlock24, perBlock16 := subnetConnLimits(tt.maxPeerCount)
		require.Equal(t, tt.perBlock24, perBlock24, "maxPeerCount=%d", tt.maxPeerCount)
		require.Equal(t, tt.perBlock16, perBlock16, "maxPeerCount=%d", tt.maxPeerCount)
	}
}

func TestResourceManagerCapsConnectionsPerIPv4Slash24(t *testing.T) {
	o := newConnOpener(t)
	perBlock24, _ := subnetConnLimits(testMaxPeerCount)
	for i := range perBlock24 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/203.0.113.%d", i+1)))
	}
	require.ErrorContains(t, o.open("/ip4/203.0.113.200"), errPerSubnetLimit)

	o.scopes[0].Done()
	o.scopes = o.scopes[1:]
	require.NoError(t, o.open("/ip4/203.0.113.200"), "a closed connection must free its slot")
}

func TestResourceManagerCapsConnectionsPerIPv4Slash16(t *testing.T) {
	o := newConnOpener(t)
	_, perBlock16 := subnetConnLimits(testMaxPeerCount)
	for i := range perBlock16 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/198.51.%d.1", i)))
	}
	require.ErrorContains(t, o.open("/ip4/198.51.200.1"), errPerSubnetLimit)
}

func TestResourceManagerCapsConnectionsPerIPv6Slash32(t *testing.T) {
	o := newConnOpener(t)
	_, perBlock16 := subnetConnLimits(testMaxPeerCount)
	for i := range perBlock16 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:%x::1", i)))
	}
	require.ErrorContains(t, o.open("/ip6/2001:db8:ff00::1"), errPerSubnetLimit)
}

func TestResourceManagerKeepsDefaultPerIPCap(t *testing.T) {
	o := newConnOpener(t)
	for range 8 {
		require.NoError(t, o.open("/ip4/203.0.113.7"))
	}
	require.ErrorContains(t, o.open("/ip4/203.0.113.7"), errPerSubnetLimit)
}

// Nodes on one LAN or docker network, for example with --caplin.local-discovery, are not capped
// per block.
func TestResourceManagerDoesNotCapPrivateAndLoopbackNetworks(t *testing.T) {
	o := newConnOpener(t)
	_, perBlock16 := subnetConnLimits(testMaxPeerCount)
	// One IPv6 /56 per connection: go-libp2p also rate-limits each /56, private or not.
	for _, format := range []string{"/ip4/10.0.0.%d", "/ip4/172.16.0.%d", "/ip4/192.168.1.%d", "/ip4/100.64.0.%d", "/ip4/169.254.1.%d", "/ip6/fd00:0:0:%x00::1"} {
		for i := range perBlock16 + 4 {
			require.NoError(t, o.open(fmt.Sprintf(format, i+1)), format)
		}
		o.closeAll() // keep few scopes open at once, below the transient-scope limits of small machines
	}
	for range perBlock16 + 4 {
		require.NoError(t, o.open("/ip4/127.0.0.1"))
	}
}
