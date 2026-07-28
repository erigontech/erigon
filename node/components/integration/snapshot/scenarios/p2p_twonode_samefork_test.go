//go:build p2p_integration

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

// Two-node same-fork bidirectional convergence. Both nodes seed
// byte-identical content (the same fork's file set) and each
// publishes ITS OWN per-node V2 manifest — the chain.v2.<enr-fp>.<seq>
// sidecar is bound to the node's enode key, so two independent nodes
// on the same fork spec produce different sidecar infohashes even
// with byte-identical underlying files. Each stamps chain-toml on
// its own ENR pointing at its own sidecar; each observes the other's
// chain-toml over the wire; each fetches the other's V2 sidecar and
// publishes flow.PeerManifestReceived — feeding the canonical-view
// aggregator on both sides so quorum can form across the pair.
//
// Mirrors the live E2E ran 2026-07-28 against fork-child-e2e +
// fork-B-e2e datadirs: cross-connected initiators, each fetches the
// other's per-node V2 (different infohashes, same underlying set),
// both publish PeerManifestReceived.

package scenarios_test

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// TestP2P_TwoNode_SameFork_BidirectionalConvergence: each node stamps
// its OWN chain-toml on its OWN ENR and the pair converges via mutual
// V2 manifest fetch. Both hold the same fixture file (byte-identical
// content, same info-hash on the shared torrent); the per-node V2
// sidecars differ because the sidecar filename embeds the node's
// enode fingerprint.
func TestP2P_TwoNode_SameFork_BidirectionalConvergence(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	nodeA := harness.NewP2PNode(t, logger)
	nodeB := harness.NewP2PNode(t, logger)

	// Two nodes on the same fork spec must produce DISTINCT per-node V2
	// sidecars (each keyed on its enode fingerprint) — that's the shape
	// real deployments hit and the reason the canonical-view aggregator
	// meaningfully receives more than one manifest to run quorum over.
	// The shared harnessENRFP default collapses both to the same
	// filename + infohash; opt into per-node fingerprints here.
	nodeA.UsePerNodeENRFingerprint()
	nodeB.UsePerNodeENRFingerprint()

	// Both seed byte-identical content — the shared fixture stands in
	// for the fork's snapshot file set. Same infohash on both because
	// the file bytes are identical; each node's local torrent client
	// verifies the same hash and can serve the same torrent.
	fixtureName := "v1.0-accounts.0-2048.kv"
	const fixtureSize = 2 << 20
	fixtureContent := multiPieceFixtureBytes("same-fork-fixture", fixtureSize)
	hashA := nodeA.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, 0, 2048)
	hashB := nodeB.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, 0, 2048)
	require.Equal(t, hashA, hashB, "byte-identical seeds must produce the same torrent infohash")

	// Each publishes ITS OWN V2 manifest — chain.v2.<enr-fp>.<seq> is
	// keyed by the node's enode fingerprint, so the two sidecars have
	// different filenames + different infohashes even though the
	// underlying inventory content is the same.
	v2HashA := nodeA.PublishV2Manifest()
	v2HashB := nodeB.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2HashA)
	require.NotEqual(t, [20]byte{}, v2HashB)
	require.NotEqual(t, v2HashA, v2HashB, "each per-node V2 sidecar must have its own infohash")

	// Each stamps its OWN chain-toml (pointing at its OWN V2 sidecar)
	// on its OWN ENR. Peer resolves the ENR on inbound handshake +
	// mx.onPeerConnected pulls chain-toml → triggers fetch of the
	// advertised infohash.
	_, btPortA := nodeA.LocalTorrentAddr()
	_, btPortB := nodeB.LocalTorrentAddr()
	nodeA.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2HashA, V2InfoHash: v2HashA, DomainSteps: 2048, MergeDepth: 2048})
	nodeA.SetDevP2PENREntry(enr.BT(btPortA))
	nodeB.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2HashB, V2InfoHash: v2HashB, DomainSteps: 2048, MergeDepth: 2048})
	nodeB.SetDevP2PENREntry(enr.BT(btPortB))

	// Observers for the assertion budget: each side must see at least
	// ONE PeerManifestReceived from the other.
	var (
		aReceived atomic.Int32
		bReceived atomic.Int32
	)
	require.NoError(t, nodeA.Bus.Subscribe(func(flow.PeerManifestReceived) { aReceived.Add(1) }))
	require.NoError(t, nodeB.Bus.Subscribe(func(flow.PeerManifestReceived) { bReceived.Add(1) }))

	// Bidirectional mesh setup — the pre-register pass is what the
	// multi-node RegisterStaticPeer fix (commit 348b4ca56e) enables:
	// both peers' static-peer maps are populated synchronously with
	// zero dials in flight, so setupConn's inbound branch on either
	// end resolves the dialer's full ENR (chain-toml intact) instead
	// of falling back to a stub.
	nodeA.PreRegisterDevP2PPeer(nodeB.DevP2PSelf())
	nodeB.PreRegisterDevP2PPeer(nodeA.DevP2PSelf())
	nodeA.AddDevP2PPeer(nodeB.DevP2PSelf())
	nodeB.AddDevP2PPeer(nodeA.DevP2PSelf())
	nodeA.AddSeederPeer(nodeB)
	nodeB.AddSeederPeer(nodeA)

	waitForP2P(t, func() bool { return aReceived.Load() >= 1 && bReceived.Load() >= 1 },
		30*time.Second, "each node received the other's V2 manifest")

	require.GreaterOrEqual(t, aReceived.Load(), int32(1), "A received B's manifest")
	require.GreaterOrEqual(t, bReceived.Load(), int32(1), "B received A's manifest")
}
