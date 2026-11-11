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

package overlay

import (
	"crypto/ecdsa"
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

var genesisHash = common.HexToHash("0x0102")

// sentryFork sets the eth entry of srv's record, as the sentry does from its
// status.
func sentryFork(srv *p2p.Server, genesis common.Hash) {
	srv.LocalNode().Set(eth.CurrentENREntryFromForks(nil, nil, genesis, 0, 0))
}

func devp2pServer(t *testing.T, k byte, discovery bool, bootnodes ...*enode.Node) *p2p.Server {
	srv := &p2p.Server{Config: p2p.Config{
		PrivateKey:       key(k),
		Name:             "eez-overlay-test",
		MaxPeers:         10,
		MaxPendingPeers:  10,
		ListenAddr:       "127.0.0.1:0",
		NoDiscovery:      !discovery,
		DiscoveryV5:      discovery,
		BootstrapNodesV5: bootnodes,
		TmpDir:           t.TempDir(),
	}}
	require.NoError(t, srv.Start(t.Context(), log.New()))
	t.Cleanup(srv.Stop)
	return srv
}

func TestENRCarriesOverlayPort(t *testing.T) {
	srv := devp2pServer(t, 1, false)
	sentryFork(srv, genesisHash)
	n := newTestNode(t, 1)
	o, err := New(Config{Listen: n.listen(), Key: key(1), Domain: testDomain, Signer: addr(0), Devp2p: srv}, log.New())
	require.NoError(t, err)
	require.NoError(t, o.Start())
	t.Cleanup(func() { require.NoError(t, o.Stop()) })

	rec := srv.LocalNode().Node()
	var port Entry
	require.NoError(t, rec.Load(&port))
	require.Equal(t, Entry(n.port), port)
	var raw rlp.RawValue
	require.NoError(t, rec.Load(enr.WithEntry("eez", &raw)))
	want, err := rlp.EncodeToBytes(uint16(n.port))
	require.NoError(t, err)
	require.Equal(t, want, []byte(raw))
	var fork ethEntry
	require.NoError(t, rec.Load(&fork), "the eth entry stays")
	require.Equal(t, forkid.NewIDFromForks(nil, nil, genesisHash, 0, 0), fork.ForkID)

	// The entry is the RLP of the port as an integer.
	for p, enc := range map[uint16]string{80: "0x50", 128: "0x8180", 9100: "0x82238c", 65535: "0x82ffff"} {
		b, err := rlp.EncodeToBytes(Entry(p))
		require.NoError(t, err)
		require.Equal(t, enc, hexutil.Encode(b), "port %d", p)
	}
}

// record returns a node record of key k on 127.0.0.1 with entries, which
// may replace the address.
func record(t *testing.T, k *ecdsa.PrivateKey, entries ...enr.Entry) *enode.Node {
	var r enr.Record
	r.Set(enr.IPv4(net.IPv4(127, 0, 0, 1)))
	r.Set(enr.UDP(30303))
	r.Set(enr.TCP(30303))
	for _, e := range entries {
		r.Set(e)
	}
	require.NoError(t, enode.SignV4(&r, k))
	n, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return n
}

func TestOverlayAddrRequiresEntryAndFork(t *testing.T) {
	ours := forkid.NewIDFromForks(nil, nil, genesisHash, 0, 0)
	k := key(5)
	sentryEntry := eth.CurrentENREntryFromForks(nil, nil, genesisHash, 0, 0)

	// The peer id is the one the node's own key gives it on the overlay.
	priv, err := p2pcrypto.UnmarshalSecp256k1PrivateKey(crypto.FromECDSA(k))
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	p, ok := overlayAddr(record(t, k, sentryEntry, Entry(9100)), ours)
	require.True(t, ok)
	require.Equal(t, peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/9100")}}, p)

	// A private IPv4 beside a global IPv6: discovery prefers the IPv6
	// endpoint, the overlay dials the ip entry.
	dual := record(t, k, sentryEntry, Entry(9100),
		enr.IPv4Addr(netip.MustParseAddr("10.0.0.1")), enr.IPv6Addr(netip.MustParseAddr("2001:db8::1")))
	require.True(t, dual.IPAddr().Is6())
	p, ok = overlayAddr(dual, ours)
	require.True(t, ok)
	require.Equal(t, peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/10.0.0.1/tcp/9100")}}, p)

	var v6 enr.Record
	v6.Set(enr.IPv6Addr(netip.MustParseAddr("2001:db8::1")))
	v6.Set(sentryEntry)
	v6.Set(Entry(9100))
	require.NoError(t, enode.SignV4(&v6, k))
	v6Only, err := enode.New(enode.ValidSchemes, &v6)
	require.NoError(t, err)
	_, ok = overlayAddr(v6Only, ours)
	require.False(t, ok, "no ip entry")

	for name, n := range map[string]*enode.Node{
		"no eez entry":    record(t, k, sentryEntry),
		"no eth entry":    record(t, k, Entry(9100)),
		"other fork hash": record(t, k, ethEntry{ForkID: forkid.ID{Hash: [4]byte{1, 2, 3, 4}}}, Entry(9100)),
		"other fork next": record(t, k, ethEntry{ForkID: forkid.ID{Hash: ours.Hash, Next: 9}}, Entry(9100)),
	} {
		_, ok := overlayAddr(n, ours)
		require.False(t, ok, name)
	}
}

func TestPeersFoundThroughDiscovery(t *testing.T) {
	t.Parallel()
	boot := devp2pServer(t, 1, true)
	servers := []*p2p.Server{boot, devp2pServer(t, 2, true, boot.Self()), devp2pServer(t, 3, true, boot.Self())}
	// Its fork id differs: it is in the tables but no overlay dials it.
	other := devp2pServer(t, 4, true, boot.Self())
	sentryFork(other, common.HexToHash("0x0103"))

	start := func(srv *p2p.Server, k byte) *Overlay {
		n := newTestNode(t, k)
		o, err := New(Config{Listen: n.listen(), Key: key(k), Domain: testDomain, Signer: addr(0), Devp2p: srv}, log.New())
		require.NoError(t, err)
		require.NoError(t, o.Start())
		t.Cleanup(func() { require.NoError(t, o.Stop()) })
		return o
	}
	var overlays []*Overlay
	for i, srv := range servers {
		sentryFork(srv, genesisHash)
		overlays = append(overlays, start(srv, byte(i+1)))
	}
	outsider := start(other, 4)

	require.Eventually(t, func() bool {
		for _, o := range overlays {
			if o.Peers() != len(overlays)-1 {
				return false
			}
		}
		return true
	}, 2*deadline, 200*time.Millisecond, "overlays did not find each other")

	// Once every table holds the outsider's record with its eez entry, a
	// dial tick later still nobody has dialled it.
	require.Eventually(t, func() bool {
		for _, srv := range servers {
			if !advertised(srv, other.Self().ID()) {
				return false
			}
		}
		return true
	}, 2*deadline, 200*time.Millisecond, "the outsider is not in every table")
	time.Sleep(DialInterval + time.Second)
	require.Zero(t, outsider.Peers(), "an overlay of another fork was dialled")
	for _, o := range overlays {
		require.Equal(t, len(overlays)-1, o.Peers())
	}

	publishHeads(t.Context(), overlays[0], 200*time.Millisecond)
	awaitCounter(t, overlays[2].Heads(), 5)
}

// advertised reports whether srv's discv5 table holds the record of id with
// an eez entry.
func advertised(srv *p2p.Server, id enode.ID) bool {
	for _, n := range srv.DiscV5().AllNodes() {
		var port Entry
		if n.ID() == id && n.Load(&port) == nil {
			return true
		}
	}
	return false
}

func TestDiscoveredBudget(t *testing.T) {
	n := newTestNode(t, 1)
	o, err := New(Config{Listen: n.listen(), Key: key(1), Domain: testDomain, Signer: addr(0)}, log.New())
	require.NoError(t, err)
	h, err := newHost(key(1), n.listen())
	require.NoError(t, err)
	defer h.Close()
	o.host = h
	found := []peer.AddrInfo{{ID: o.ID()}}
	for i := range DiscoveryPeerLimit + 4 {
		id, err := PeerID(&key(byte(10 + i)).PublicKey)
		require.NoError(t, err)
		found = append(found, peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/1")}})
	}
	o.discover = func() []peer.AddrInfo { return found }
	first := o.discovered()
	require.Len(t, first, DiscoveryPeerLimit, "the budget of a node without connections")
	require.NotContains(t, first, found[0], "the node itself")
	require.Equal(t, found[1:DiscoveryPeerLimit+1], first)
	require.Equal(t, append(slices.Clone(found[DiscoveryPeerLimit+1:]), found[1:13]...), o.discovered(), "the next tick starts past the first")
}

func TestNextBatchRotates(t *testing.T) {
	var cursor int
	items := []int{1, 2, 3}
	require.Equal(t, []int{1, 2}, nextBatch(items, &cursor, 2))
	require.Equal(t, []int{3, 1}, nextBatch(items, &cursor, 2))
	require.Equal(t, []int{2, 3}, nextBatch(items, &cursor, 2))
	require.Equal(t, []int{1, 2, 3}, items, "items unchanged")
	require.Empty(t, nextBatch([]int(nil), &cursor, 2))
	require.Equal(t, 16, DiscoveryPeerLimit)
}
