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
	"crypto/rand"
	"fmt"
	"net"
	"net/netip"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// The tests use a small peer pool, where the /16 cap is still larger than the /24 cap.
const testMaxPeerCount = 64

type connOpener struct {
	rm     network.ResourceManager
	scopes []network.ConnManagementScope
}

func newConnOpener(t *testing.T) *connOpener {
	return newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount})
}

func newConnOpenerWithConfig(t *testing.T, cfg *P2PConfig) *connOpener {
	rm, err := newResourceManager(cfg)
	require.NoError(t, err)
	o := &connOpener{rm: rm}
	t.Cleanup(func() {
		o.closeAll()
		require.NoError(t, rm.Close())
	})
	return o
}

// open opens a connection scope from addr ("/ip4/..." or "/ip6/..."). Both directions count
// against the caps; alternating them keeps each direction below go-libp2p's transient-scope
// limits, which are 32 connections on a machine with little memory.
func (o *connOpener) open(addr string) error {
	dir := network.DirInbound
	if len(o.scopes)%2 == 1 {
		dir = network.DirOutbound
	}
	scope, err := o.rm.OpenConnection(dir, false, multiaddr.StringCast(addr+"/tcp/30303"))
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

func TestIPBlockConnLimitsScaleWithPeerCount(t *testing.T) {
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
		perBlock24, perBlock16 := ipBlockConnLimits(tt.maxPeerCount)
		require.Equal(t, tt.perBlock24, perBlock24, "maxPeerCount=%d", tt.maxPeerCount)
		require.Equal(t, tt.perBlock16, perBlock16, "maxPeerCount=%d", tt.maxPeerCount)
	}
}

// The lists are broadest first, so a refusal leaves no zero-count entry for a narrower prefix. An
// IPv6 /48 keeps go-libp2p's cap of 64 at most.
func TestIPBlockLimitsAreBroadestFirst(t *testing.T) {
	prefixLengths := func(limits []rcmgr.ConnLimitPerSubnet) []int {
		lengths := make([]int, 0, len(limits))
		for _, limit := range limits {
			lengths = append(lengths, limit.PrefixLength)
		}
		return lengths
	}
	for _, tt := range []struct {
		maxPeerCount uint64
		perSlash48   int
	}{
		{maxPeerCount: 128, perSlash48: 16},
		{maxPeerCount: 512, perSlash48: 64},
		{maxPeerCount: 1024, perSlash48: 64},
	} {
		v4, v6 := ipBlockLimits(tt.maxPeerCount)
		require.Equal(t, []int{16, 24, 32}, prefixLengths(v4))
		require.Equal(t, []int{32, 48, 56}, prefixLengths(v6))
		require.Equal(t, tt.perSlash48, v6[1].ConnCount, "maxPeerCount=%d", tt.maxPeerCount)
	}
}

func TestResourceManagerCapsConnectionsPerIPv4Slash24(t *testing.T) {
	o := newConnOpener(t)
	perBlock24, _ := ipBlockConnLimits(testMaxPeerCount)
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
	_, perBlock16 := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock16 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/198.51.%d.1", i)))
	}
	require.ErrorContains(t, o.open("/ip4/198.51.200.1"), errPerSubnetLimit)
}

// An IPv6 /48 is a single end site, like an IPv4 /24, and gets the same cap.
func TestResourceManagerCapsConnectionsPerIPv6Slash48(t *testing.T) {
	o := newConnOpener(t)
	perBlock24, _ := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock24 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:1:%x00::1", i)))
	}
	require.ErrorContains(t, o.open("/ip6/2001:db8:1:ff00::1"), errPerSubnetLimit)
}

func TestResourceManagerCapsConnectionsPerIPv6Slash32(t *testing.T) {
	o := newConnOpener(t)
	_, perBlock16 := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock16 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:%x::1", i)))
	}
	require.ErrorContains(t, o.open("/ip6/2001:db8:ff00::1"), errPerSubnetLimit)
}

func TestResourceManagerKeepsDefaultPerAddressCaps(t *testing.T) {
	o := newConnOpener(t)
	for range 8 {
		require.NoError(t, o.open("/ip4/203.0.113.7"))
	}
	require.ErrorContains(t, o.open("/ip4/203.0.113.7"), errPerSubnetLimit)
	for i := range 8 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:0:1::%x", i+1)))
	}
	require.ErrorContains(t, o.open("/ip6/2001:db8:0:1::ff"), errPerSubnetLimit)
}

// Without --caplin.local-discovery private addresses keep the released per-address caps.
func TestResourceManagerCapsPrivateAddressesWithoutLocalDiscovery(t *testing.T) {
	o := newConnOpener(t)
	for _, addr := range []string{"/ip4/10.0.0.7", "/ip6/fd00::7"} {
		for range 8 {
			require.NoError(t, o.open(addr), addr)
		}
		require.ErrorContains(t, o.open(addr), errPerSubnetLimit, addr)
		o.closeAll()
	}
}

// With --caplin.local-discovery, nodes on one LAN or docker network are capped neither per block
// nor per address.
func TestResourceManagerDoesNotCapPrivateNetworksWithLocalDiscovery(t *testing.T) {
	o := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount, LocalDiscovery: true})
	_, perBlock16 := ipBlockConnLimits(testMaxPeerCount)
	// One IPv6 /56 per connection: go-libp2p also rate-limits each /56, private or not.
	for _, format := range []string{"/ip4/10.0.0.%d", "/ip4/172.16.0.%d", "/ip4/192.168.1.%d", "/ip4/100.64.0.%d", "/ip4/169.254.1.%d", "/ip6/fd00:0:0:%x00::1", "/ip6/fe80:0:0:%x00::1"} {
		for i := range perBlock16 + 4 {
			require.NoError(t, o.open(fmt.Sprintf(format, i+1)), format)
		}
		o.closeAll()
	}
	for _, addr := range []string{"/ip4/10.0.0.7", "/ip6/fd00::7"} {
		for range 12 {
			require.NoError(t, o.open(addr), addr)
		}
		o.closeAll()
	}
}

// Loopback keeps go-libp2p's unlimited default, also when exempt networks replace the default list.
// A trusted network inside loopback must not replace go-libp2p's unlimited loopback entry with
// the trusted limit, 32 at this peer count.
func TestResourceManagerDoesNotCapLoopback(t *testing.T) {
	for _, cfg := range []*P2PConfig{
		{MaxPeerCount: testMaxPeerCount},
		{MaxPeerCount: testMaxPeerCount, LocalDiscovery: true},
		{MaxPeerCount: testMaxPeerCount, TrustedCIDRs: []string{"203.0.113.0/24", "2001:db8:aa::/48"}},
		{MaxPeerCount: 4, TrustedCIDRs: []string{"127.0.0.1/32", "::1/128"}},
	} {
		o := newConnOpenerWithConfig(t, cfg)
		for _, addr := range []string{"/ip4/127.0.0.1", "/ip6/::1"} {
			for range 40 {
				require.NoError(t, o.open(addr), addr)
			}
			o.closeAll()
		}
	}
}

// Exempt networks still share one finite limit, derived from the peer pool.
func TestResourceManagerLimitsExemptNetworkTogether(t *testing.T) {
	const maxPeerCount = 4
	o := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: maxPeerCount, LocalDiscovery: true})
	limit := exemptNetworkConnLimit(maxPeerCount)
	for i := range limit {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/10.0.%d.%d", i/200, i%200+1)))
	}
	require.ErrorContains(t, o.open("/ip4/10.1.0.1"), errPerSubnetLimit)
}

func TestExemptNetworkConnLimitCoversThePeerPool(t *testing.T) {
	require.Equal(t, 32, exemptNetworkConnLimit(0))
	require.Equal(t, 1024, exemptNetworkConnLimit(128))
	require.Equal(t, 16384, exemptNetworkConnLimit(2048))
}

// QUIC asks an unverified source to prove its address once it exceeds a per-network burst; the
// exemption from the caps must not remove that burst.
func TestResourceManagerKeepsQUICSourceVerificationForExemptNetworks(t *testing.T) {
	rm := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount, LocalDiscovery: true}).rm
	for _, ip := range []string{"10.0.0.1", "fd00::1"} {
		addr := &net.UDPAddr{IP: net.ParseIP(ip), Port: 30303}
		verified := false
		for range exemptNetworkConnLimit(testMaxPeerCount) {
			if rm.VerifySourceAddress(addr) {
				verified = true
				break
			}
		}
		require.True(t, verified, ip)
	}
}

func TestResourceManagerDoesNotCapTrustedNetworks(t *testing.T) {
	o := newConnOpenerWithConfig(t, &P2PConfig{
		MaxPeerCount: testMaxPeerCount,
		// A space after the comma and an IPv4-mapped form, as an operator may write them.
		TrustedCIDRs: []string{"203.0.113.0/24", " 2001:db8:aa::/48", "::ffff:198.51.100.0/120"},
	})
	_, perBlock16 := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock16 + 4 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/203.0.113.%d", i+1)))
	}
	o.closeAll()
	for i := range perBlock16 + 4 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:aa:%x00::1", i)))
	}
	o.closeAll()
	for _, addr := range []string{"/ip4/203.0.113.7", "/ip4/198.51.100.7"} {
		for range 12 {
			require.NoError(t, o.open(addr), addr)
		}
		o.closeAll()
	}

	perBlock24, _ := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock24 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/192.0.2.%d", i+1)))
	}
	require.ErrorContains(t, o.open("/ip4/192.0.2.200"), errPerSubnetLimit, "other public blocks stay capped")
}

func TestParseTrustedCIDRs(t *testing.T) {
	got, err := ParseTrustedCIDRs([]string{"203.0.113.0/24", "", " ", " 2001:db8::/32 ", "::ffff:198.51.100.0/120"})
	require.NoError(t, err)
	require.Equal(t, []netip.Prefix{
		netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("2001:db8::/32"),
		netip.MustParsePrefix("198.51.100.0/24"),
	}, got)

	for _, cidr := range []string{"203.0.113.1", "203.0.113.5/8", "0.0.0.0/0", "10.0.0.0/7", "::/0", "2000::/15", "2001:db8::1/32"} {
		_, err := ParseTrustedCIDRs([]string{cidr})
		require.Error(t, err, cidr)
	}
}

// --caplin.ip-block-limits.disable gives exactly the released limits, also for private networks.
func TestResourceManagerDisableIPBlockLimitsKeepsReleasedLimits(t *testing.T) {
	o := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount, DisableIPBlockLimits: true, LocalDiscovery: true})
	_, perBlock16 := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock16 + 4 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/203.0.113.%d", i+1)), "no /24 or /16 cap")
	}
	o.closeAll()
	for i := range perBlock16 + 4 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:%x::1", i+1)), "no IPv6 /32 cap")
	}
	o.closeAll()
	for i := range 64 / 2 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:1:%x00::1", i)), "released IPv6 /48 cap of 64")
	}
	o.closeAll()
	for _, addr := range []string{"/ip4/198.51.100.7", "/ip4/10.0.0.7", "/ip6/fd00::7"} {
		for range 8 {
			require.NoError(t, o.open(addr), addr)
		}
		require.ErrorContains(t, o.open(addr), errPerSubnetLimit, addr)
		o.closeAll()
	}
}

// Refusals by the caps are not counted by go-libp2p's metrics and are logged only at Debug, so the
// node keeps its own per-block summary.
func TestRefusalCounterSummarisesRefusedBlocks(t *testing.T) {
	o := newConnOpener(t)
	counter := newRefusalCounter(o.rm)
	o.rm = counter
	perBlock24, _ := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock24 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/203.0.113.%d", i+1)))
	}
	for range 3 {
		require.ErrorContains(t, o.open("/ip4/203.0.113.200"), errPerSubnetLimit)
	}
	for range 8 {
		require.NoError(t, o.open("/ip6/2001:db8:0:1::7"))
	}
	require.ErrorContains(t, o.open("/ip6/2001:db8:0:1::7"), errPerSubnetLimit)
	_, err := counter.OpenConnection(network.DirInbound, false, multiaddr.StringCast("/ip4/203.0.113.201/udp/30303/quic-v1"))
	require.ErrorContains(t, err, errPerSubnetLimit, "QUIC is refused and counted like TCP")

	total, mostRefusedBlocks := counter.flush()
	require.Equal(t, 5, total)
	require.Equal(t, "203.0.113.0/24: 4, 2001:db8::/48: 1", mostRefusedBlocks)
	total, _ = counter.flush()
	require.Zero(t, total)
}

func TestResourceManagerCloseTwice(t *testing.T) {
	rm, err := newResourceManager(&P2PConfig{MaxPeerCount: testMaxPeerCount})
	require.NoError(t, err)
	require.NoError(t, rm.Close())
	require.NotPanics(t, func() { _ = rm.Close() })
}

// staticPeer returns a direct TCP libp2p address for ip, as --sentinel.staticpeers takes it.
func staticPeer(t *testing.T, proto, ip string) string {
	key, _, err := crypto.GenerateSecp256k1Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(key)
	require.NoError(t, err)
	return fmt.Sprintf("/%s/%s/tcp/9000/p2p/%s", proto, ip, id)
}

// The operator configured these peers; a full block must not keep refusing them.
func TestResourceManagerDoesNotCapStaticPeersByBlock(t *testing.T) {
	netCfg := &clparams.NetworkConfig{StaticPeers: []string{
		staticPeer(t, "ip4", "203.0.113.200"),
		staticPeer(t, "ip6", "2001:db8:1:ff00::1"),
	}}
	o := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount, NetworkConfig: netCfg})
	perBlock24, _ := ipBlockConnLimits(testMaxPeerCount)
	for i := range perBlock24 {
		require.NoError(t, o.open(fmt.Sprintf("/ip4/203.0.113.%d", i+1)))
	}
	require.NoError(t, o.open("/ip4/203.0.113.200"))
	require.ErrorContains(t, o.open("/ip4/203.0.113.201"), errPerSubnetLimit, "other addresses in the block stay capped")
	o.closeAll()

	for i := range perBlock24 {
		require.NoError(t, o.open(fmt.Sprintf("/ip6/2001:db8:1:%x00::1", i)))
	}
	require.NoError(t, o.open("/ip6/2001:db8:1:ff00::1"))
	require.ErrorContains(t, o.open("/ip6/2001:db8:1:fe00::1"), errPerSubnetLimit)
}

func TestResourceManagerKeepsPerAddressCapForStaticPeers(t *testing.T) {
	netCfg := &clparams.NetworkConfig{StaticPeers: []string{staticPeer(t, "ip4", "203.0.113.200")}}
	o := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount, NetworkConfig: netCfg})
	for range 8 {
		require.NoError(t, o.open("/ip4/203.0.113.200"))
	}
	require.Error(t, o.open("/ip4/203.0.113.200"))
}

// go-libp2p matches the most specific network first, so a static peer's own entry must not
// lower the allowance of a trusted network that contains it.
func TestResourceManagerStaticPeerInTrustedNetworkKeepsTrustedLimit(t *testing.T) {
	netCfg := &clparams.NetworkConfig{StaticPeers: []string{staticPeer(t, "ip4", "203.0.113.7")}}
	o := newConnOpenerWithConfig(t, &P2PConfig{
		MaxPeerCount: testMaxPeerCount, NetworkConfig: netCfg, TrustedCIDRs: []string{"203.0.113.0/24"},
	})
	for range 12 {
		require.NoError(t, o.open("/ip4/203.0.113.7"))
	}
}

// A static peer on loopback must not replace go-libp2p's unlimited loopback entry with a cap.
func TestResourceManagerDoesNotCapLoopbackStaticPeers(t *testing.T) {
	netCfg := &clparams.NetworkConfig{StaticPeers: []string{
		staticPeer(t, "ip4", "127.0.0.1"),
		staticPeer(t, "ip6", "::1"),
	}}
	o := newConnOpenerWithConfig(t, &P2PConfig{MaxPeerCount: testMaxPeerCount, NetworkConfig: netCfg})
	for _, addr := range []string{"/ip4/127.0.0.1", "/ip6/::1"} {
		for range 20 {
			require.NoError(t, o.open(addr), addr)
		}
	}
}
