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

// Mid-merge joiner scenario. Two seeders advertise the same logical
// coverage at different merge depths:
//
//   peer A — constituents only:  domain/v1.0-accounts.0-32.kv
//                                domain/v1.0-accounts.32-64.kv
//   peer B — merged only:        domain/v1.0-accounts.0-64.kv
//
// A late joiner C arrives with empty inventory. Both A and B are valid
// sources for accounts coverage at [0, 64); the swarm must converge to
// a state where every peer holds every canonical file.
//
// Open question this test surfaces: with the orchestrator's current
// range-based gap-fill, does C download both forms or just one? The
// preferring-merged selection logic would belong in a future validator
// or selector layer; today we observe and document what the orchestrator
// does so we know what to design against.

package scenarios_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// midMergeFixture describes one file in the synthetic fixture: its
// relative path, role classification (used by the harness preverified
// parser via the matching regex), and the bytes to write.
type midMergeFile struct {
	relPath string
	bytes   []byte
}

// buildMidMergeFixture writes a small synthetic snapshot dir to baseDir
// containing constituents + merged forms for one state domain, plus
// minimal block, meta, and salt files. preverified.toml is built by
// computing each file's torrent infohash via the production
// BuildTorrentIfNeed path and writing the canonical (rel-path → hex)
// map. Returns the full path to the fixture's snapshots dir.
//
// Total disk: a few hundred KiB. Runs in milliseconds.
func buildMidMergeFixture(t *testing.T, baseDir string) string {
	t.Helper()
	snapDir := filepath.Join(baseDir, "snapshots")
	require.NoError(t, os.MkdirAll(filepath.Join(snapDir, "domain"), 0o755))

	// Deterministic content seeded so reruns produce identical infohashes.
	stable := func(seed string, n int) []byte {
		// SHA-256-style stretch so bytes are well-mixed but not reproducible
		// from a fixed integer seed across Go versions; use a predictable
		// pattern instead.
		out := make([]byte, n)
		for i := 0; i < n; i++ {
			out[i] = byte(i*31+len(seed)*7) ^ seed[i%len(seed)]
		}
		return out
	}

	// Two constituents at canonical [0, 32) and [32, 64); each 8 KiB.
	// One merged file at canonical [0, 64); 16 KiB. Sizes differ so
	// even an accidental hash collision (effectively impossible at
	// SHA-1 scale, but the assertion-side comparison is by infohash)
	// stays diagnosable.
	files := []midMergeFile{
		{"domain/v1.0-accounts.0-32.kv", stable("constituent-low", 8<<10)},
		{"domain/v1.0-accounts.32-64.kv", stable("constituent-high", 8<<10)},
		{"domain/v1.0-accounts.0-64.kv", stable("merged-0-64", 16<<10)},
		{"v1.0-000000-000100-headers.seg", stable("headers-block", 8<<10)},
		{"v1.0-000000-000100-bodies.seg", stable("bodies-block", 8<<10)},
		{"erigondb.toml", []byte("step_size = 32\nsteps_in_frozen_file = 64\n")},
	}
	// Salt files: real Erigon writes 4 bytes of random; mirror the shape.
	saltBlocks := make([]byte, 4)
	saltState := make([]byte, 4)
	_, err := rand.Read(saltBlocks)
	require.NoError(t, err)
	_, err = rand.Read(saltState)
	require.NoError(t, err)
	files = append(files,
		midMergeFile{"salt-blocks.txt", saltBlocks},
		midMergeFile{"salt-state.txt", saltState},
	)

	for _, f := range files {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, f.relPath), f.bytes, 0o644))
	}

	// Compute torrent infohashes via the production path so the test
	// can write a preverified.toml that the harness parser will accept.
	torrentFS := dl.NewAtomicTorrentFS(snapDir)
	preverified := make(map[string]string, len(files))
	for _, f := range files {
		_, err := dl.BuildTorrentIfNeed(context.Background(), f.relPath, snapDir, torrentFS)
		require.NoError(t, err, "build .torrent for %s", f.relPath)
		spec, err := torrentFS.LoadByName(f.relPath + ".torrent")
		require.NoError(t, err, "load .torrent for %s", f.relPath)
		preverified[f.relPath] = hex.EncodeToString(spec.InfoHash[:])
	}

	// Sort for deterministic file content (the harness parser doesn't
	// require sorted, but it's nice for diffability).
	keys := make([]string, 0, len(preverified))
	for k := range preverified {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "'%s' = '%s'\n", k, preverified[k])
	}
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "preverified.toml"), []byte(sb.String()), 0o644))

	return snapDir
}

// TestP2P_Swarm_MidMergeJoiner exposes how the swarm handles a state
// where one peer holds the unmerged constituents at [0, 32) + [32, 64)
// while another peer holds the merged equivalent at [0, 64). A late
// joiner arrives empty.
//
// **Documented behaviour today** (this test pins it down):
//
//   - The orchestrator's gap-fill is range-based per file role.
//     `[0, 32)+[32, 64)` and `[0, 64)` are equivalent at the
//     step-range layer for the kv role; once a peer holds either
//     form, its coverage is complete and no further pulls fire.
//
//   - Result: peers A and B do NOT exchange their divergent forms.
//     Each retains its own seeded set + the common files. Peer C
//     (late joiner) picks ONE of the two paths (whichever advertiser's
//     manifest the orchestrator processes first) and stops once that
//     coverage is satisfied.
//
//   - Convergence is reached at the transport layer (every peer
//     covers the full step range for every role) but NOT at the
//     file-name layer (peers hold different physical files for the
//     same logical coverage).
//
// **What this test ASSERTS**:
//
//   - Each peer's seeded files compute infohashes that match the
//     synthesized preverified.toml (open-loop check 1).
//   - Each peer's meta / salt / block files all converge (those
//     don't have merge ambiguity).
//   - Each peer reaches range-coverage completeness for the
//     accounts domain (covers [0, 64) via either form).
//   - Byte-equality where the file names overlap.
//
// **What it explicitly does NOT assert**:
//
//   - File-name uniformity (no peer is required to hold both
//     constituents AND the merged form, nor is anyone required to
//     prefer the merged).
//
// Tightening this to "every peer ends with the merged canonical
// form" is the storage validation phase's job. When that lands, the
// merge-equivalence validator will:
//
//   1. Recognise that constituents at [0, 32)+[32, 64) and merged at
//      [0, 64) cover the same logical state for the same domain.
//   2. Apply the configured canonical-form policy (default: prefer
//      merged) to mark one form as canonical, the other for
//      retirement.
//   3. Drive the orchestrator to fetch the canonical form if the
//      peer doesn't already hold it.
//
// This test then becomes the regression check for that path.
func TestP2P_Swarm_MidMergeJoiner(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	baseDir := t.TempDir()
	srcDir := buildMidMergeFixture(t, baseDir)

	entries, err := harness.LoadPreverified(srcDir)
	require.NoError(t, err)

	// Filter to V2-covered primaries (skip derived files there are none
	// here, but the predicate keeps the test honest).
	var primaries []harness.PreverifiedEntry
	for _, e := range entries {
		if e.Role.IsPrimary() {
			primaries = append(primaries, e)
		}
	}
	require.NotEmpty(t, primaries, "fixture must contain at least one primary file")

	// Manual partition: peer A gets the two constituents, peer B gets
	// the merged. Both peers also get the meta/salt and block files
	// (those have no merge-state dimension in this test). Late joiner
	// C starts empty.
	type partition struct {
		idx       int
		ownsRoles map[string]bool // true = this peer holds the file
	}
	const (
		idxA = 0
		idxB = 1
		idxC = 2
	)
	common := map[string]bool{
		"v1.0-000000-000100-headers.seg": true,
		"v1.0-000000-000100-bodies.seg":  true,
		"erigondb.toml":                  true,
		"salt-blocks.txt":                true,
		"salt-state.txt":                 true,
	}
	parts := []partition{
		{idx: idxA, ownsRoles: map[string]bool{
			"domain/v1.0-accounts.0-32.kv":  true,
			"domain/v1.0-accounts.32-64.kv": true,
		}},
		{idx: idxB, ownsRoles: map[string]bool{
			"domain/v1.0-accounts.0-64.kv": true,
		}},
		{idx: idxC, ownsRoles: nil}, // empty
	}
	for i := range parts {
		if parts[i].ownsRoles == nil {
			continue
		}
		for k, v := range common {
			parts[i].ownsRoles[k] = v
		}
	}

	type peerEntry struct {
		node          *harness.P2PNode
		manifestCount *atomic.Int32
		promotedCount *atomic.Int32
	}
	peers := make([]*peerEntry, 3)

	bringUp := func(p partition) *peerEntry {
		t.Helper()
		peerDir := filepath.Join(baseDir, fmt.Sprintf("peer-%d", p.idx))
		require.NoError(t, os.MkdirAll(peerDir, 0o755))
		node := harness.NewP2PNodeAt(t, peerDir, logger)

		seeded := 0
		for _, e := range primaries {
			if !p.ownsRoles[e.RelPath] {
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
			require.Equal(t, e.InfoHash, got, "open-loop check 1 FAILED for peer %d %s", p.idx, e.RelPath)
			seeded++
		}

		// All peers publish a V2 — even the empty one. Empty V2 is fine
		// (it just won't advertise much). Not strictly necessary for
		// the late joiner, but matches the staggered-entry shape.
		v2 := node.PublishV2Manifest()
		_, btPort := node.LocalTorrentAddr()
		node.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2})
		node.SetDevP2PENREntry(enr.BT(btPort))
		node.EnableAutoPublishV2(1 * time.Second)

		entry := &peerEntry{node: node, manifestCount: &atomic.Int32{}, promotedCount: &atomic.Int32{}}
		mc := entry.manifestCount
		pc := entry.promotedCount
		require.NoError(t, node.Bus.Subscribe(func(flow.PeerManifestReceived) { mc.Add(1) }))
		require.NoError(t, node.Bus.Subscribe(func(flow.TrustPromoted) { pc.Add(1) }))

		t.Logf("peer %d up: seeded %d files (%s)", p.idx, seeded, partitionLabel(p.idx))
		return entry
	}

	meshAdd := func(upTo int) {
		t.Helper()
		for i := 0; i <= upTo; i++ {
			for j := 0; j <= upTo; j++ {
				if i == j || peers[i] == nil || peers[j] == nil {
					continue
				}
				peers[i].node.AddDevP2PPeer(peers[j].node.DevP2PSelf())
			}
		}
		for i := 0; i <= upTo; i++ {
			for j := 0; j <= upTo; j++ {
				if i == j || peers[i] == nil || peers[j] == nil {
					continue
				}
				peers[i].node.AddSeederPeer(peers[j].node)
			}
		}
		// Synth-rebroadcast — see comment in p2p_staggered_entry_test.go.
		for i := 0; i <= upTo; i++ {
			for j := 0; j <= upTo; j++ {
				if i == j || peers[i] == nil || peers[j] == nil {
					continue
				}
				peers[i].node.Sentry.PublishPeerConnected(peers[j].node.DevP2PSelf())
			}
		}
	}

	// Bring up peer A and peer B at t=0; let them stabilise. Both should
	// already have the canonical files in their respective subsets so
	// nothing actually transfers between A and B yet (their advertised
	// coverages are disjoint at the file-name level — A doesn't have
	// 0-64.kv, B doesn't have 0-32.kv or 32-64.kv).
	t.Logf("=== bringing up peer A (constituents) and peer B (merged) ===")
	peers[idxA] = bringUp(parts[idxA])
	peers[idxB] = bringUp(parts[idxB])
	meshAdd(idxB)

	// Brief settle period — let A and B exchange manifests + finish any
	// cross-pulls before C arrives.
	time.Sleep(10 * time.Second)

	t.Logf("=== bringing up peer C (empty late joiner) ===")
	peers[idxC] = bringUp(parts[idxC])
	meshAdd(idxC)

	// Wait for transport-layer convergence: every peer's pending list
	// is empty AND every common-set file is on disk AND the accounts
	// domain has full [0, 64) range coverage via SOME form.
	timeout := 2 * time.Minute
	transferStart := time.Now()
	t.Logf("waiting up to %s for transport-layer convergence", timeout)

	commonSet := make(map[string]bool, len(common))
	for k := range common {
		commonSet[k] = true
	}

	hasFile := func(snap string, rel string) bool {
		_, err := os.Stat(filepath.Join(snap, rel))
		return err == nil
	}
	hasAccountsCoverage := func(snap string) bool {
		// Either the merged form OR both constituents satisfy [0, 64).
		merged := hasFile(snap, "domain/v1.0-accounts.0-64.kv")
		c1 := hasFile(snap, "domain/v1.0-accounts.0-32.kv")
		c2 := hasFile(snap, "domain/v1.0-accounts.32-64.kv")
		return merged || (c1 && c2)
	}
	allCommonPresent := func(snap string) bool {
		for k := range commonSet {
			if !hasFile(snap, k) {
				return false
			}
		}
		return true
	}

	lastLog := time.Time{}
	waitForP2P(t, func() bool {
		converged := true
		for _, pe := range peers {
			if pe.node.Orch.PendingCount() != 0 {
				converged = false
			}
			if !hasAccountsCoverage(pe.node.Dirs.Snap) {
				converged = false
			}
			if !allCommonPresent(pe.node.Dirs.Snap) {
				converged = false
			}
		}
		if !converged && time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			for i, pe := range peers {
				t.Logf("[progress %s] peer %d: files-on-disk=%d/%d pending=%d manifests=%d promoted=%d",
					time.Since(transferStart).Round(time.Second), i,
					countFilesUnder(t, pe.node.Dirs.Snap, primaries), len(primaries),
					pe.node.Orch.PendingCount(), pe.manifestCount.Load(), pe.promotedCount.Load())
			}
		}
		return converged
	}, timeout, "every peer covers [0, 64) and holds all common files")

	t.Logf("transport-layer convergence reached in %.2fs", time.Since(transferStart).Seconds())

	// Byte-equality on every file each peer DOES hold (we don't assert
	// uniformity of which files, but every file present must match).
	srcHashes := make(map[string][32]byte, len(primaries))
	for _, e := range primaries {
		h, err := sha256File(filepath.Join(srcDir, e.RelPath))
		require.NoError(t, err)
		srcHashes[e.RelPath] = h
	}
	for i, pe := range peers {
		for _, e := range primaries {
			peerPath := filepath.Join(pe.node.Dirs.Snap, e.RelPath)
			if _, err := os.Stat(peerPath); os.IsNotExist(err) {
				continue // peer chose not to hold this file — fine for mid-merge
			}
			gotHash, err := sha256File(peerPath)
			require.NoError(t, err, "peer %d %s", i, e.RelPath)
			require.Equal(t, srcHashes[e.RelPath], gotHash,
				"byte-equality FAILED for peer %d %s", i, e.RelPath)
		}
	}

	// Diagnostic: surface which form each peer ended up with so the
	// "preferring merged" question is visible in the test trace. When
	// the validation phase adds canonical-form policy this output is
	// what the regression check tightens against.
	for i, pe := range peers {
		merged := hasFile(pe.node.Dirs.Snap, "domain/v1.0-accounts.0-64.kv")
		c1 := hasFile(pe.node.Dirs.Snap, "domain/v1.0-accounts.0-32.kv")
		c2 := hasFile(pe.node.Dirs.Snap, "domain/v1.0-accounts.32-64.kv")
		form := "neither"
		switch {
		case merged && c1 && c2:
			form = "BOTH (merged + constituents)"
		case merged:
			form = "merged only"
		case c1 && c2:
			form = "constituents only"
		}
		t.Logf("[final] peer %d (%s): accounts form = %s; full inventory = [%s]",
			i, partitionLabel(i), form, inventorySummary(pe.node.Inventory))
	}
}

// partitionLabel returns a human-readable label for the test peer
// partitions. Used in logs to make traces self-descriptive.
func partitionLabel(idx int) string {
	switch idx {
	case 0:
		return "constituents only"
	case 1:
		return "merged only"
	case 2:
		return "empty late joiner"
	}
	return "unknown"
}

// inventorySummary lists the canonical-file names in an inventory's
// domains/blocks/meta/salt/caplin slices for diagnostic logs.
func inventorySummary(inv *snapshot.Inventory) string {
	var names []string
	for _, d := range inv.Domains() {
		for _, f := range inv.LocalFiles(d) {
			names = append(names, f.Name)
		}
	}
	for _, f := range inv.BlockFiles() {
		names = append(names, f.Name)
	}
	for _, f := range inv.MetaFiles() {
		names = append(names, f.Name)
	}
	for _, f := range inv.SaltFiles() {
		names = append(names, f.Name)
	}
	for _, f := range inv.CaplinFiles() {
		names = append(names, f.Name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
