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

// Three-node merge-divergence scenario through the real DevP2P stack
// and real anacrolix/torrent transport. Peer A seeds merged accounts
// files covering [0, 500); peer B seeds the same range split into five
// unmerged slices. Leecher C connects to both and must reach complete
// coverage [0, 500) regardless of which peer's handshake wins the race
// to announce first.

package scenarios_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
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

// Ranges are power-of-2 aligned so GenerateV2's canonicity filter
// accepts them — non-canonical entries get dropped from the V2 manifest
// and the two peers would then produce byte-identical (empty)
// manifests with the same infohash.
const (
	mergedFrom     uint64 = 0
	mergedTo       uint64 = 512
	unmergedWidth  uint64 = 128
	unmergedSlices        = 4 // 4 × 128 = 512 total coverage
)

// seedMergedPeer writes merged accounts files (one kv + one kvi
// covering [0, 512)), publishes the V2 manifest, and returns the
// infohash of the manifest plus a map of filename → on-disk content so
// the leecher's received copy can be byte-compared against the source.
func seedMergedPeer(t *testing.T, node *harness.P2PNode) ([20]byte, map[string][]byte) {
	t.Helper()
	const size = 16 << 10 // 16 KiB per file
	contents := make(map[string][]byte, 2)
	kv := fmt.Sprintf("v1.0-accounts.%d-%d.kv", mergedFrom, mergedTo)
	kvi := fmt.Sprintf("v1.0-accounts.%d-%d.kvi", mergedFrom, mergedTo)
	contents[kv] = multiPieceFixtureBytes("merged-kv", size)
	contents[kvi] = multiPieceFixtureBytes("merged-kvi", size)

	node.SeedFile(kv, contents[kv], snapshot.DomainAccounts, mergedFrom, mergedTo)
	node.SeedFile(kvi, contents[kvi], snapshot.DomainAccounts, mergedFrom, mergedTo)
	return node.PublishV2Manifest(), contents
}

// seedUnmergedPeer writes 4 × 128-wide slices across accounts .kv and
// .kvi (eight files total), publishes the V2 manifest, and returns the
// infohash plus the filename→content map for byte-equality checks.
func seedUnmergedPeer(t *testing.T, node *harness.P2PNode) ([20]byte, map[string][]byte) {
	t.Helper()
	const size = 16 << 10
	contents := make(map[string][]byte, 2*int(unmergedSlices))
	for i := uint64(0); i < unmergedSlices; i++ {
		from := i * unmergedWidth
		to := (i + 1) * unmergedWidth
		kv := fmt.Sprintf("v1.0-accounts.%d-%d.kv", from, to)
		kvi := fmt.Sprintf("v1.0-accounts.%d-%d.kvi", from, to)
		contents[kv] = multiPieceFixtureBytes(kv, size)
		contents[kvi] = multiPieceFixtureBytes(kvi, size)
		node.SeedFile(kv, contents[kv], snapshot.DomainAccounts, from, to)
		node.SeedFile(kvi, contents[kvi], snapshot.DomainAccounts, from, to)
	}
	return node.PublishV2Manifest(), contents
}

// setENR publishes the peer's ChainToml + BT port on its sentry
// provider's live LocalNode so connecting peers see the advertisement
// in the signed ENR during the real eth/68 handshake.
func setENR(t *testing.T, node *harness.P2PNode, v2Hash [20]byte, coverageSteps, mergeDepth uint64) {
	t.Helper()
	_, btPort := node.LocalTorrentAddr()
	node.SetDevP2PENREntry(enr.ChainToml{
		AuthoritativeBlocks: 0,
		KnownBlocks:         0,
		InfoHash:            v2Hash,
		DomainSteps:         coverageSteps,
		MergeDepth:          mergeDepth,
	})
	node.SetDevP2PENREntry(enr.BT(btPort))
}

// TestP2P_ThreeNode_MergeDivergence_RealStack runs the merge-divergence
// scenario over real DevP2P + real torrent. Ordering of peer
// handshakes isn't controllable; the orchestrator must still reach
// complete coverage for accounts [0, 500) regardless of which peer
// "wins" the first manifest-received event.
func TestP2P_ThreeNode_MergeDivergence_RealStack(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	peerA := harness.NewP2PNode(t, logger) // merged
	peerB := harness.NewP2PNode(t, logger) // unmerged
	leecher := harness.NewP2PNode(t, logger)

	mergedHash, mergedContents := seedMergedPeer(t, peerA)
	setENR(t, peerA, mergedHash, mergedTo, mergedTo)

	unmergedHash, unmergedContents := seedUnmergedPeer(t, peerB)
	setENR(t, peerB, unmergedHash, unmergedSlices*unmergedWidth, unmergedWidth)

	// Leecher needs BT static peers for both seeders so new torrents
	// can reach whichever has the file.
	leecher.AddSeederPeer(peerA)
	leecher.AddSeederPeer(peerB)

	var (
		manifestCount atomic.Int32
		promotedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	// Kick off both handshakes simultaneously through real sentry. The
	// relative ordering is unpredictable on real DevP2P — that's the
	// point of the test.
	leecher.AddDevP2PPeer(peerA.DevP2PSelf())
	leecher.AddDevP2PPeer(peerB.DevP2PSelf())

	// Wait for gap-fill to settle: at least one manifest received, and
	// the orchestrator's pending-map drained, AND the promoted count
	// has been stable for a quiet window. The orchestrator doesn't
	// emit an explicit completion signal, so quiescence is the proxy.
	waitForP2P(t, func() bool {
		return manifestCount.Load() >= 2 && leecher.Orch.PendingCount() == 0 && promotedCount.Load() >= 2
	}, 30*time.Second, "both manifests received + all downloads settled")
	// Small additional wait to catch any last-moment late download.
	time.Sleep(100 * time.Millisecond)

	require.GreaterOrEqual(t, manifestCount.Load(), int32(2),
		"both A and B must have had their manifests fetched")

	// Coverage at TrustVerified must span [0, 512) regardless of which
	// peer's files C ended up with. The orchestrator either took A's
	// single merged pair or B's four-way split — both valid outcomes.
	accountsCov := leecher.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: mergedTo}}, accountsCov,
		"accounts coverage must be complete")

	accountsFiles := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
	const expectedMerged = 2                    // one .kv + one .kvi
	expectedUnmerged := 2 * int(unmergedSlices) // kv + kvi per slice
	var expectedContents map[string][]byte
	switch len(accountsFiles) {
	case expectedMerged:
		for _, f := range accountsFiles {
			require.Equal(t, mergedFrom, f.FromStep)
			require.Equal(t, mergedTo, f.ToStep)
		}
		expectedContents = mergedContents
		t.Logf("leecher took the merged path (%d files from peer A)", expectedMerged)
	case expectedUnmerged:
		expectedContents = unmergedContents
		t.Logf("leecher took the unmerged path (%d files from peer B)", expectedUnmerged)
	default:
		t.Fatalf("unexpected accounts file count: %d (expected %d or %d)",
			len(accountsFiles), expectedMerged, expectedUnmerged)
	}

	// No failures.
	for _, f := range accountsFiles {
		require.Equal(t, snapshot.TrustVerified, f.Trust)
	}

	// Byte-equality: the leecher's on-disk copies match the source bytes
	// from whichever peer actually served them. Proves real data
	// transferred through the full three-node + real-DevP2P pipeline.
	for _, f := range accountsFiles {
		want, ok := expectedContents[f.Name]
		require.True(t, ok, "unexpected file on leecher: %s", f.Name)
		got, err := os.ReadFile(filepath.Join(leecher.Dirs.Snap, f.Name))
		require.NoError(t, err, "reading %s on leecher", f.Name)
		require.Equal(t, sha256.Sum256(want), sha256.Sum256(got),
			"content mismatch for %s", f.Name)
	}
}
