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

// Staggered swarm entry. Same open-loop reference as TestP2P_Swarm_FullReplication
// (real preverified.toml against a real snapshot dir), but peers don't all
// join at t=0 — a first wave starts the swarm and later peers join at
// staggered intervals carrying their own seeded slice. Late joiners must:
//
//  1. Discover the existing swarm at the moment they arrive — see CURRENT
//     V2 manifests of existing peers, not the manifests they had at t=0.
//     Requires the auto-republisher (BindAutoPublish) to keep each peer's
//     ENR in sync with its growing inventory.
//  2. Catch up to the full archive while continuing to seed their own
//     slice to existing peers.
//  3. Converge to the same byte-identical state on disk as the early
//     joiners, with the same regenerated V2 toml across all peers.
//
// Skipped unless SNAPSHOT_DIR is set. SNAPSHOT_PEERS overrides the peer
// count (default 5). SNAPSHOT_STAGGER_INTERVAL controls the gap between
// late-joiner waves (default 60s).
//
//	SNAPSHOT_DIR=/erigon/hoodi-test-fixture/snapshots \
//	SNAPSHOT_PEERS=5 \
//	go test -tags=p2p_integration -timeout 4h -v \
//	  -run TestP2P_Swarm_StaggeredEntry \
//	  ./node/components/integration/snapshot/scenarios/

package scenarios_test

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// TestP2P_Swarm_StaggeredEntry covers staggered peer entry. The first
// wave of peers (size = numPeers - lateJoiners) starts at t=0, owning
// the round-robin partition of preverified.toml primaries. Each
// subsequent late-joiner peer arrives after SNAPSHOT_STAGGER_INTERVAL,
// carrying its own slice. Final state: every peer has every primary on
// disk, all open-loop checks pass.
func TestP2P_Swarm_StaggeredEntry(t *testing.T) {
	srcDir := os.Getenv("SNAPSHOT_DIR")
	if srcDir == "" {
		t.Skip("SNAPSHOT_DIR not set; skipping staggered-entry test")
	}

	entries, err := harness.LoadPreverified(srcDir)
	require.NoError(t, err)

	var present []harness.PreverifiedEntry
	for _, e := range entries {
		if !e.Role.IsPrimary() {
			continue
		}
		if _, err := os.Stat(filepath.Join(srcDir, e.RelPath)); err != nil {
			continue
		}
		present = append(present, e)
	}
	require.NotEmpty(t, present, "no V2-covered primary files found on disk under %s", srcDir)

	var totalBytes int64
	for _, e := range present {
		st, err := os.Stat(filepath.Join(srcDir, e.RelPath))
		require.NoError(t, err)
		totalBytes += st.Size()
	}

	numPeers := 5
	if v := os.Getenv("SNAPSHOT_PEERS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, 3, "SNAPSHOT_PEERS must be >= 3 (need at least one late-joiner)")
		numPeers = n
	}
	staggerInterval := 60 * time.Second
	if v := os.Getenv("SNAPSHOT_STAGGER_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoError(t, err)
		staggerInterval = d
	}

	// Wave layout: first half (rounded up) starts at t=0, the rest stagger
	// in one at a time. With numPeers=5: 3 at t=0, peer 3 at +60s, peer 4 at +120s.
	firstWave := (numPeers + 1) / 2
	lateJoiners := numPeers - firstWave
	t.Logf("source dir: %s (primaries: %d, total bytes: %.2f GiB)",
		srcDir, len(present), float64(totalBytes)/(1<<30))
	t.Logf("staggered topology: %d peers in wave 1 at t=0, %d late joiners every %s",
		firstWave, lateJoiners, staggerInterval)

	// Co-locate peer dirs with the fixture so hardlinks share inodes.
	baseRoot := filepath.Join(filepath.Dir(srcDir),
		"staggered-entry-test."+strconv.FormatInt(time.Now().UnixNano(), 10))
	require.NoError(t, os.MkdirAll(baseRoot, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(baseRoot) })

	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	// One-time hardlink + seed of every peer's slice. The seed phase doesn't
	// know about wave boundaries — every peer's data is staged before
	// any wave joins. The wave control is purely about WHEN we add the
	// peer to the swarm.
	type peerEntry struct {
		node          *harness.P2PNode
		manifestCount *atomic.Int32
		promotedCount *atomic.Int32
	}
	peers := make([]*peerEntry, numPeers)

	bringUp := func(idx int) *peerEntry {
		t.Helper()
		peerDir := filepath.Join(baseRoot, "peer-"+strconv.Itoa(idx))
		require.NoError(t, os.MkdirAll(peerDir, 0o755))
		node := harness.NewP2PNodeAt(t, peerDir, logger)

		// Hardlink + seed this peer's round-robin slice.
		seedStart := time.Now()
		seeded := 0
		for i, e := range present {
			if i%numPeers != idx {
				continue
			}
			src := filepath.Join(srcDir, e.RelPath)
			dst := filepath.Join(node.Dirs.Snap, e.RelPath)
			require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
			if err := os.Link(src, dst); err != nil {
				require.NoError(t, copyFile(src, dst), "copy %s", e.RelPath)
			}
			got := node.SeedExistingFile(e.RelPath, &snapshot.FileEntry{
				Domain: e.Domain, Kind: e.Kind, FromStep: e.FromStep, ToStep: e.ToStep,
			})
			require.Equal(t, e.InfoHash, got, "open-loop check 1 FAILED for %s", e.RelPath)
			seeded++
		}

		// Initial publish + ENR set (auto-publisher takes over from here).
		v2 := node.PublishV2Manifest()
		_, btPort := node.LocalTorrentAddr()
		node.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2})
		node.SetDevP2PENREntry(enr.BT(btPort))
		// Auto-publish is now reliable: the rolling V2 publisher keeps
		// the previous N chain.v2.<seq>.toml generations seedable, so a
		// late joiner that captured an older ENR snapshot at handshake
		// time can still fetch the infohash it asked for. 5s debounce
		// lets a fast convergence batch its TrustPromoted events but
		// stays responsive enough that late joiners see near-current
		// inventory at handshake.
		node.EnableAutoPublishV2(5 * time.Second)

		entry := &peerEntry{node: node, manifestCount: &atomic.Int32{}, promotedCount: &atomic.Int32{}}
		mc := entry.manifestCount
		pc := entry.promotedCount
		require.NoError(t, node.Bus.Subscribe(func(flow.PeerManifestReceived) { mc.Add(1) }))
		require.NoError(t, node.Bus.Subscribe(func(flow.TrustPromoted) { pc.Add(1) }))

		t.Logf("peer %d up: seeded %d files in %.2fs", idx, seeded, time.Since(seedStart).Seconds())
		return entry
	}

	meshAdd := func(newIdx int) {
		t.Helper()
		// Two-pass: first AddDevP2PPeer everywhere so every server has
		// every other peer in its static-peer table BEFORE any inbound
		// handshake completes, then AddSeederPeer for BT-level static
		// peers. See p2p/server.go:setupConn fix and the corresponding
		// comment in p2p_full_replication_test.go for why.
		for i := 0; i <= newIdx; i++ {
			for j := 0; j <= newIdx; j++ {
				if i == j || peers[i] == nil || peers[j] == nil {
					continue
				}
				peers[i].node.AddDevP2PPeer(peers[j].node.DevP2PSelf())
			}
		}
		for i := 0; i <= newIdx; i++ {
			for j := 0; j <= newIdx; j++ {
				if i == j || peers[i] == nil || peers[j] == nil {
					continue
				}
				peers[i].node.AddSeederPeer(peers[j].node)
			}
		}
	}

	// Bring up wave 1.
	t.Logf("=== bringing up wave 1 (peers 0..%d) ===", firstWave-1)
	wave1Start := time.Now()
	for i := 0; i < firstWave; i++ {
		peers[i] = bringUp(i)
	}
	meshAdd(firstWave - 1)

	// Stagger in late joiners. Between joins, log progress so the test
	// trace shows what was in flight when each new peer arrived.
	for joiner := firstWave; joiner < numPeers; joiner++ {
		stagger := staggerInterval
		t.Logf("=== sleeping %s before bringing up peer %d ===", stagger, joiner)
		time.Sleep(stagger)

		for i := 0; i < joiner; i++ {
			t.Logf("[pre-join %s] peer %d: files-on-disk=%d/%d pending=%d manifests=%d promoted=%d",
				time.Since(wave1Start).Round(time.Second), i,
				countFilesUnder(t, peers[i].node.Dirs.Snap, present), len(present),
				peers[i].node.Orch.PendingCount(),
				peers[i].manifestCount.Load(), peers[i].promotedCount.Load())
		}

		peers[joiner] = bringUp(joiner)
		meshAdd(joiner)
		t.Logf("=== peer %d joined at %s ===", joiner, time.Since(wave1Start).Round(time.Second))
	}

	// Convergence wait — every peer must hold every preverified primary
	// and have an empty pending set.
	expectedFilesPerPeer := len(present)
	const minPerPeerRateBytesPerSec = 8 * 1024 * 1024
	perPeerPull := totalBytes * int64(numPeers-1) / int64(numPeers)
	timeout := time.Duration(perPeerPull/minPerPeerRateBytesPerSec)*time.Second + 30*time.Minute
	if timeout < 2*time.Hour {
		timeout = 2 * time.Hour
	}
	t.Logf("waiting up to %s for full convergence (every peer holds all %d files)", timeout, expectedFilesPerPeer)

	progressInterval := 2 * time.Minute
	if v := os.Getenv("SNAPSHOT_PROGRESS_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			progressInterval = d
		}
	}
	transferStart := time.Now()
	lastLog := time.Time{}
	waitForP2P(t, func() bool {
		converged := true
		for i, pe := range peers {
			if pe == nil {
				converged = false
				continue
			}
			if countFilesUnder(t, pe.node.Dirs.Snap, present) < expectedFilesPerPeer {
				converged = false
			}
			if pe.node.Orch.PendingCount() != 0 {
				converged = false
			}
			_ = i
		}
		if !converged && time.Since(lastLog) > progressInterval {
			lastLog = time.Now()
			for i, pe := range peers {
				if pe == nil {
					t.Logf("[progress %s] peer %d: not yet joined",
						time.Since(transferStart).Round(time.Second), i)
					continue
				}
				t.Logf("[progress %s] peer %d: files-on-disk=%d/%d pending=%d manifests=%d promoted=%d",
					time.Since(transferStart).Round(time.Second), i,
					countFilesUnder(t, pe.node.Dirs.Snap, present), expectedFilesPerPeer,
					pe.node.Orch.PendingCount(), pe.manifestCount.Load(), pe.promotedCount.Load())
			}
		}
		return converged
	}, timeout, "every peer holds all primary files (staggered entry)")
	elapsed := time.Since(transferStart)
	t.Logf("convergence reached in %.2fs (%.1f MiB/s aggregate)",
		elapsed.Seconds(), float64(int64(numPeers-1)*totalBytes)/elapsed.Seconds()/(1<<20))

	// Open-loop check 2: byte-equality vs source.
	srcHashes := make(map[string][32]byte, len(present))
	for _, e := range present {
		h, err := sha256File(filepath.Join(srcDir, e.RelPath))
		require.NoError(t, err)
		srcHashes[e.RelPath] = h
	}
	for i, pe := range peers {
		for _, e := range present {
			gotHash, err := sha256File(filepath.Join(pe.node.Dirs.Snap, e.RelPath))
			require.NoError(t, err, "peer %d %s", i, e.RelPath)
			require.Equal(t, srcHashes[e.RelPath], gotHash,
				"open-loop check 2 FAILED for peer %d %s", i, e.RelPath)
		}
	}
	t.Logf("byte-equality confirmed across %d peers x %d files", numPeers, len(present))

	// Open-loop check 3: each peer's regenerated V2 matches every other
	// peer's, and matches preverified.toml for the V2-covered subset.
	type kindHashes map[string][20]byte
	flatten := func(m *dl.ChainTomlV2) kindHashes {
		out := kindHashes{}
		for name, hash := range m.Blocks {
			out[name] = parseHash20(t, hash)
		}
		for name, hash := range m.Meta {
			out[name] = parseHash20(t, hash)
		}
		for name, hash := range m.Salt {
			out[name] = parseHash20(t, hash)
		}
		for _, c := range m.Caplin {
			out[c.Name] = parseHash20(t, c.Hash)
		}
		for _, dm := range m.Domains {
			for _, f := range dm.Files {
				out[f.Name] = parseHash20(t, f.Hash)
			}
		}
		return out
	}
	var reference kindHashes
	for i, pe := range peers {
		got := flatten(dl.GenerateV2(pe.node.Inventory))
		if i == 0 {
			reference = got
			continue
		}
		require.Equal(t, reference, got, "peer %d V2 disagrees with peer 0", i)
	}
	preverifiedByName := make(map[string][20]byte, len(present))
	for _, e := range present {
		preverifiedByName[e.RelPath] = e.InfoHash
	}
	for name, hash := range reference {
		if want, ok := preverifiedByName[name]; ok {
			require.Equal(t, want, hash, "peer-generated V2 hash for %s differs from preverified", name)
		}
	}
	t.Logf("V2 cross-peer agreement + preverified concordance OK")
}

