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

package scenarios_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestBreadth_EmptyPeerManifest asserts that a peer announcing nothing
// causes no DownloadRequested and does not error the leecher — a
// well-behaved no-op path that can occur early in peer discovery.
func TestBreadth_EmptyPeerManifest(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	require.NoError(t, leecher.Start(context.Background()))

	// Peer with no inventory — nothing to announce, but the announce call
	// still fires a PeerManifestReceived with empty domain/block maps.
	peer := harness.NewFakePeer("empty-peer", coord)
	peer.AnnounceTo(leecher)
	leecher.Bus.WaitAsync()

	requestType := reflect.TypeOf(flow.DownloadRequested{})
	require.Equal(t, 0, leecher.Bus.CountOfType(requestType),
		"empty manifest should produce no requests")
	require.Equal(t, 0, leecher.Orch.PendingCount())
}

// TestBreadth_PartialDomainPeer asserts that when a peer advertises only a
// subset of domains (e.g. accounts + storage but not code or commitment),
// the leecher downloads what is advertised and ends up with partial
// coverage — no spurious errors for unadvertised domains.
func TestBreadth_PartialDomainPeer(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	require.NoError(t, leecher.Start(context.Background()))

	// Seed the peer with only accounts+storage; drop code, commitment, blocks.
	partial := harness.HoodiBaseline()
	delete(partial.Domains, snapshot.DomainCode)
	delete(partial.Domains, snapshot.DomainCommitment)
	partial.Blocks = nil

	peer := harness.NewFakePeer("partial", coord)
	peer.Seed(partial)
	peer.AnnounceTo(leecher)
	leecher.Bus.WaitAsync()

	// Leecher has accounts + storage, lacks code + commitment.
	require.NotEmpty(t, leecher.Inventory.LocalFiles(snapshot.DomainAccounts))
	require.NotEmpty(t, leecher.Inventory.LocalFiles(snapshot.DomainStorage))
	require.Empty(t, leecher.Inventory.LocalFiles(snapshot.DomainCode))
	require.Empty(t, leecher.Inventory.LocalFiles(snapshot.DomainCommitment))

	// No failures, no orphan pending.
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})))
	require.Equal(t, 0, leecher.Orch.PendingCount())
}

// TestBreadth_ComplementaryPeers asserts that when two peers advertise
// disjoint domains, the leecher assembles the union. Exercises that gap-fill
// proceeds across successive manifests rather than only the first.
func TestBreadth_ComplementaryPeers(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	require.NoError(t, leecher.Start(context.Background()))

	// Peer A: accounts + storage only.
	accountsSide := harness.HoodiBaseline()
	delete(accountsSide.Domains, snapshot.DomainCode)
	delete(accountsSide.Domains, snapshot.DomainCommitment)
	accountsSide.Blocks = nil

	// Peer B: code + commitment only.
	codeSide := harness.HoodiBaseline()
	delete(codeSide.Domains, snapshot.DomainAccounts)
	delete(codeSide.Domains, snapshot.DomainStorage)
	codeSide.Blocks = nil

	peerA := harness.NewFakePeer("peer-A", coord)
	peerA.Seed(accountsSide)
	peerA.AnnounceTo(leecher)

	peerB := harness.NewFakePeer("peer-B", coord)
	peerB.Seed(codeSide)
	peerB.AnnounceTo(leecher)

	leecher.Bus.WaitAsync()

	// Leecher now has coverage across all four domains.
	for _, d := range []snapshot.Domain{
		snapshot.DomainAccounts, snapshot.DomainStorage,
		snapshot.DomainCode, snapshot.DomainCommitment,
	} {
		require.NotEmpty(t, leecher.Inventory.LocalFiles(d),
			"domain %s should be covered after complementary peers", d)
	}
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})))
	require.Equal(t, 0, leecher.Orch.PendingCount())
}

// TestBreadth_OrphanDownloadComplete asserts the orchestrator's handling of a
// DownloadComplete for a file never advertised by any peer — logs a warning
// and does not add the file to local inventory. Validates the warning path.
func TestBreadth_OrphanDownloadComplete(t *testing.T) {
	coord := harness.NewCoordinator()
	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })
	require.NoError(t, leecher.AttachSimulatedTransport(coord))
	require.NoError(t, leecher.Start(context.Background()))

	before := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)

	// Publish a DownloadComplete for a file the orchestrator never saw.
	leecher.Bus.Publish(flow.DownloadComplete{
		FileName:  "v1.0-accounts.99999-99999.kv",
		InfoHash:  [20]byte{},
		LocalPath: "/sim/unknown",
		Size:      128,
	})
	leecher.Bus.WaitAsync()

	after := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Equal(t, len(before), len(after), "orphan DownloadComplete must not mutate inventory")
}

// TestBreadth_DoubleClose asserts that Close is idempotent: calling it twice
// does not error or panic. Important because Node.Close is wired into
// t.Cleanup in tests that may also explicitly call it, and long-running
// scenarios may close on shutdown paths that overlap.
func TestBreadth_DoubleClose(t *testing.T) {
	coord := harness.NewCoordinator()
	node := harness.NewNode(nil)
	require.NoError(t, node.AttachSimulatedTransport(coord))
	require.NoError(t, node.Start(context.Background()))

	require.NoError(t, node.Close())
	// Second Close should be a clean no-op — not an error.
	require.NoError(t, node.Close())
}
