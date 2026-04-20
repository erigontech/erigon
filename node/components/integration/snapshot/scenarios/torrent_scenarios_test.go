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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// newTorrentNode returns a Node wired with a TorrentTransport in its own
// temp DataDir, already Start'd. Registers a t.Cleanup for Close.
func newTorrentNode(t *testing.T, ctx context.Context) (*harness.Node, *harness.TorrentTransport) {
	t.Helper()
	node := harness.NewNode(nil)
	t.Cleanup(func() { _ = node.Close() })

	transport, err := harness.NewTorrentTransport(node.Bus, t.TempDir())
	require.NoError(t, err)
	node.Transport = transport
	require.NoError(t, node.Start(ctx))
	return node, transport
}

// TestTorrent_TwoNodeGapFillFullFixture ports TestTwoNode_GapFillMergedOnly
// onto real torrent transport. Exercises concurrent multi-file transfer
// through the orchestrator — every file in HoodiBaseline goes over real
// TCP between two in-process clients.
func TestTorrent_TwoNodeGapFillFullFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("torrent-transport tests skipped under -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seeder, seederT := newTorrentNode(t, ctx)
	leecher, leecherT := newTorrentNode(t, ctx)

	fixture := harness.HoodiBaseline()
	require.NoError(t, harness.SeedFixtureAsTorrentPeer(seederT, seeder.Inventory, fixture))

	leecherT.AddPeer("seeder", "127.0.0.1", seederT.LocalPort())
	leecher.Bus.Publish(harness.BuildPeerManifest("seeder", seeder.Inventory))

	expected := fixture.FileCount()
	require.Eventually(t, func() bool {
		return leecherLocalFileCount(leecher) == expected
	}, 60*time.Second, 50*time.Millisecond, "leecher should receive every seeded file")

	for _, d := range seeder.Inventory.Domains() {
		require.Equal(t,
			seeder.Inventory.Coverage(d),
			leecher.Inventory.CoverageAtTrust(d, snapshot.TrustVerified),
			"coverage mismatch for domain %s", d)
	}
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})),
		"no downloads should fail in the happy path")
}

// TestTorrent_ThreeNode_MergeDivergence ports TestThreeNode_MergeDivergence
// onto real torrent. Two seeders advertise the same step range in different
// granularities (merged vs unmerged); the leecher's role-coverage dedup
// should land it on the merged seeder's files when the merged manifest
// arrives first, without ever trying to download the unmerged slices.
func TestTorrent_ThreeNode_MergeDivergence(t *testing.T) {
	if testing.Short() {
		t.Skip("torrent-transport tests skipped under -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	leecher, leecherT := newTorrentNode(t, ctx)
	merged, mergedT := newTorrentNode(t, ctx)
	unmerged, unmergedT := newTorrentNode(t, ctx)

	require.NoError(t, harness.SeedFixtureAsTorrentPeer(mergedT, merged.Inventory, harness.HoodiMerged500()))
	require.NoError(t, harness.SeedFixtureAsTorrentPeer(unmergedT, unmerged.Inventory, harness.HoodiUnmerged500()))

	leecherT.AddPeer("merged", "127.0.0.1", mergedT.LocalPort())
	leecherT.AddPeer("unmerged", "127.0.0.1", unmergedT.LocalPort())

	// Merged first — so the leecher's pending/local covers accounts [0,500)
	// before the unmerged manifest is processed, and unmerged slices are
	// deduped out by coverageForRoleLocked.
	leecher.Bus.Publish(harness.BuildPeerManifest("merged", merged.Inventory))
	leecher.Bus.Publish(harness.BuildPeerManifest("unmerged", unmerged.Inventory))

	mergedFileCount := harness.HoodiMerged500().FileCount()
	require.Eventually(t, func() bool {
		return leecherLocalFileCount(leecher) == mergedFileCount
	}, 90*time.Second, 50*time.Millisecond, "leecher should complete merged peer's files")

	// Accounts coverage matches the merged peer — single [0, 500) range.
	accountsCov := leecher.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 500}}, accountsCov)

	// Exactly the merged peer's accounts files live locally — two entries
	// (.kv + .kvi), not the unmerged peer's ten.
	require.Len(t, leecher.Inventory.LocalFiles(snapshot.DomainAccounts), 2,
		"leecher should hold exactly the merged peer's accounts files")
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})))
}

func leecherLocalFileCount(n *harness.Node) int {
	total := len(n.Inventory.BlockFiles())
	for _, d := range n.Inventory.Domains() {
		total += len(n.Inventory.LocalFiles(d))
	}
	return total
}
