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

// Open-loop full-replication test. The reference is a real Erigon snapshot
// dir produced by running the production sync pipeline against Hoodi up to
// the txnum-index phase (see /erigon/mark/build-hoodi-test-fixture.sh). Its
// preverified.toml is the authoritative (filename → infohash) set, computed
// by the production publishing pipeline — NOT by this test. The test:
//
//   - Partitions the V2-covered subset (block .seg + domain primaries +
//     caplin .seg + meta + salt) round-robin across N mixed peers.
//   - Each peer hardlinks its slice into its own snap dir and seeds it.
//     The first open-loop check: the harness's locally-computed infohash
//     must match the preverified hash for every seeded file.
//   - Mesh-connects every peer to every other; runs the swarm to
//     convergence.
//   - Second open-loop check: every peer's on-disk content matches the
//     source byte-for-byte.
//   - Third open-loop check: every peer independently regenerates a V2
//     toml; all N agree across peers AND match preverified.toml for the
//     V2-covered subset.
//
// Skipped unless SNAPSHOT_DIR is set. Time budget scales with content
// size — pass a generous -timeout. Default fixture is the artefact at
// /erigon/hoodi-test-fixture/snapshots produced by the wrapper script.
//
// Invocation:
//
//	SNAPSHOT_DIR=/erigon/hoodi-test-fixture/snapshots \
//	go test -tags=p2p_integration -timeout 6h -v \
//	  -run TestP2P_Swarm_FullReplication \
//	  ./node/components/integration/snapshot/scenarios/

package scenarios_test

import (
	"crypto/sha256"
	"io"
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

// TestP2P_Swarm_FullReplication is the open-loop, full-archive proof that
// the V2-covered subset of preverified.toml replicates correctly across a
// cooperating swarm. See file-level comment for the open-loop checks.
func TestP2P_Swarm_FullReplication(t *testing.T) {
	srcDir := os.Getenv("SNAPSHOT_DIR")
	if srcDir == "" {
		t.Skip("SNAPSHOT_DIR not set; skipping full-replication test")
	}

	entries, err := harness.LoadPreverified(srcDir)
	require.NoError(t, err)

	// Filter to V2-covered primaries that actually exist on disk in the
	// fixture. Derived files (accessor, idx) are not transferred — they
	// rebuild locally from primaries via BuildMissedAccessors. Caplin
	// archives are primaries but the fixture may not have them on disk
	// (Caplin's retire-to-seg pipeline hadn't run when we stopped erigon).
	var present []harness.PreverifiedEntry
	var missing int
	for _, e := range entries {
		if !e.Role.IsPrimary() {
			continue
		}
		_, err := os.Stat(filepath.Join(srcDir, e.RelPath))
		if err != nil {
			missing++
			continue
		}
		present = append(present, e)
	}
	require.NotEmpty(t, present, "no V2-covered primary files found on disk under %s", srcDir)

	roleCounts := map[harness.PreverifiedRole]int{}
	var totalBytes int64
	for _, e := range present {
		roleCounts[e.Role]++
		st, err := os.Stat(filepath.Join(srcDir, e.RelPath))
		require.NoError(t, err)
		totalBytes += st.Size()
	}
	t.Logf("source dir: %s", srcDir)
	t.Logf("primaries: %d present, %d preverified-but-not-on-disk (e.g. caplin without retire)", len(present), missing)
	for role, n := range roleCounts {
		t.Logf("  %s: %d files", role, n)
	}
	t.Logf("total bytes to replicate per leecher: %.2f GiB", float64(totalBytes)/(1<<30))

	numPeers := 3
	if v := os.Getenv("SNAPSHOT_PEERS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "SNAPSHOT_PEERS must be an integer")
		require.GreaterOrEqual(t, n, 2, "SNAPSHOT_PEERS must be ≥ 2")
		numPeers = n
	}

	// Co-locate peer dirs with the fixture so hardlinks share inodes
	// rather than full content copies.
	baseRoot := filepath.Join(filepath.Dir(srcDir), "full-replication-test."+strconv.FormatInt(time.Now().UnixNano(), 10))
	require.NoError(t, os.MkdirAll(baseRoot, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(baseRoot) })

	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	peers := make([]*harness.P2PNode, numPeers)
	for i := range peers {
		peerDir := filepath.Join(baseRoot, "peer-"+strconv.Itoa(i))
		require.NoError(t, os.MkdirAll(peerDir, 0o755))
		peers[i] = harness.NewP2PNodeAt(t, peerDir, logger)
	}

	// --- Open-loop check 1: seeded infohash matches preverified --------
	//
	// Round-robin partition. Each peer hardlinks its slice in (cross-fs
	// fallback to copy), seeds, captures the harness-computed infohash,
	// and asserts it matches preverified.toml. If this fails, the
	// publishing pipeline is producing different infohashes than the
	// production registry — drops the test.
	t.Logf("seeding %d primaries across %d peers (round-robin partition)...", len(present), numPeers)
	seedStart := time.Now()
	for i, e := range present {
		peerIdx := i % numPeers
		src := filepath.Join(srcDir, e.RelPath)
		dst := filepath.Join(peers[peerIdx].Dirs.Snap, e.RelPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
		if err := os.Link(src, dst); err != nil {
			require.NoError(t, copyFile(src, dst), "copy %s", e.RelPath)
		}
		got := peers[peerIdx].SeedExistingFile(e.RelPath, &snapshot.FileEntry{
			Domain:   e.Domain,
			Kind:     e.Kind,
			FromStep: e.FromStep,
			ToStep:   e.ToStep,
		})
		require.Equal(t, e.InfoHash, got,
			"open-loop check 1 FAILED for %s: harness computed %x, preverified.toml says %x",
			e.RelPath, got, e.InfoHash)
	}
	t.Logf("seed + hash agreement: %d files in %.2fs", len(present), time.Since(seedStart).Seconds())

	// Each peer publishes its V2 toml + ENR.
	for _, p := range peers {
		v2 := p.PublishV2Manifest()
		_, btPort := p.LocalTorrentAddr()
		p.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2})
		p.SetDevP2PENREntry(enr.BT(btPort))
	}

	// Subscribers on each peer to observe progress.
	manifestCounts := make([]*atomic.Int32, numPeers)
	promotedCounts := make([]*atomic.Int32, numPeers)
	for i, p := range peers {
		manifestCounts[i] = &atomic.Int32{}
		promotedCounts[i] = &atomic.Int32{}
		mc := manifestCounts[i]
		pc := promotedCounts[i]
		require.NoError(t, p.Bus.Subscribe(func(flow.PeerManifestReceived) { mc.Add(1) }))
		require.NoError(t, p.Bus.Subscribe(func(flow.TrustPromoted) { pc.Add(1) }))
	}

	// Mesh-connect every peer to every other at BT + DevP2P layers.
	for i, p := range peers {
		for j, other := range peers {
			if i == j {
				continue
			}
			p.AddSeederPeer(other)
			p.AddDevP2PPeer(other.DevP2PSelf())
		}
	}

	// --- Wait for convergence ------------------------------------------
	//
	// Every peer must end up with every primary entry on disk. Convergence
	// is conservative: PendingCount==0 isn't sufficient — a peer that
	// already had a file (its own seeded slice) reports pending==0 trivially.
	// Walk each peer's snap dir and count.
	expectedFilesPerPeer := len(present)
	timeout := time.Duration(totalBytes)/30 + 10*time.Minute // ≥ 30 MiB/s, plus 10m floor
	if timeout < 30*time.Minute {
		timeout = 30 * time.Minute
	}
	t.Logf("waiting up to %s for full convergence (every peer holds all %d files)", timeout, expectedFilesPerPeer)

	transferStart := time.Now()
	waitForP2P(t, func() bool {
		for _, p := range peers {
			if countFilesUnder(t, p.Dirs.Snap, present) < expectedFilesPerPeer {
				return false
			}
			if p.Orch.PendingCount() != 0 {
				return false
			}
		}
		return true
	}, timeout, "every peer holds all primary files")
	elapsed := time.Since(transferStart)
	t.Logf("convergence reached in %.2fs (%.1f MiB/s)", elapsed.Seconds(), float64(totalBytes)/elapsed.Seconds()/(1<<20))
	for i := range peers {
		t.Logf("peer %d: manifests=%d promoted=%d", i, manifestCounts[i].Load(), promotedCounts[i].Load())
	}

	// --- Open-loop check 2: byte-equality against source ---------------
	//
	// Every peer's on-disk file matches the source byte-for-byte. Use
	// SHA-256 over a streamed read so multi-GB files don't blow memory.
	srcHashes := make(map[string][32]byte, len(present))
	for _, e := range present {
		h, err := sha256File(filepath.Join(srcDir, e.RelPath))
		require.NoError(t, err)
		srcHashes[e.RelPath] = h
	}
	for i, p := range peers {
		for _, e := range present {
			peerPath := filepath.Join(p.Dirs.Snap, e.RelPath)
			gotHash, err := sha256File(peerPath)
			require.NoError(t, err, "peer %d %s sha256", i, e.RelPath)
			require.Equal(t, srcHashes[e.RelPath], gotHash,
				"open-loop check 2 FAILED for peer %d %s", i, e.RelPath)
		}
	}
	t.Logf("byte-equality confirmed across %d peers × %d files", numPeers, len(present))

	// --- Open-loop check 3: each peer's regenerated V2 matches all -----
	//
	// Every peer independently calls GenerateV2 against its own
	// converged inventory. The (filename → infohash) pairs must agree
	// across peers AND match preverified.toml for the V2-covered subset.
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
	for i, p := range peers {
		got := flatten(dl.GenerateV2(p.Inventory))
		if i == 0 {
			reference = got
			continue
		}
		require.Equal(t, reference, got,
			"open-loop check 3 FAILED: peer %d V2 disagrees with peer 0", i)
	}

	// Cross-check the (filename → infohash) pairs against preverified.
	// Only V2-covered roles participate; the test treats preverified as
	// authoritative for those names.
	preverifiedByName := make(map[string][20]byte, len(present))
	for _, e := range present {
		preverifiedByName[e.RelPath] = e.InfoHash
	}
	for name, hash := range reference {
		if want, ok := preverifiedByName[name]; ok {
			require.Equal(t, want, hash,
				"open-loop check 3 FAILED: peer-generated V2 hash for %s differs from preverified", name)
		}
	}
	t.Logf("V2 cross-peer agreement + preverified concordance OK")
}

// copyFile copies src to dst with O_EXCL — if the destination already
// exists, the link or prior copy attempt did the right thing already
// and we don't want to overwrite. Uses io.Copy so multi-GB files
// don't allocate full content in memory.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// sha256File streams src through sha256 without buffering the whole
// content — multi-GB primaries would OOM otherwise.
func sha256File(path string) ([32]byte, error) {
	var out [32]byte
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return out, err
	}
	copy(out[:], h.Sum(nil))
	return out, nil
}

// countFilesUnder returns the number of preverified entries that exist
// (size>0 OK) under root.
func countFilesUnder(t *testing.T, root string, entries []harness.PreverifiedEntry) int {
	t.Helper()
	count := 0
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(root, e.RelPath)); err == nil {
			count++
		}
	}
	return count
}

// parseHash20 decodes a 40-char hex string from a V2 manifest into a
// fixed-size info-hash. Test-side helper — V2 stores hashes as hex.
func parseHash20(t *testing.T, s string) [20]byte {
	t.Helper()
	require.Len(t, s, 40, "v2 hash must be 40 hex chars, got %q", s)
	var out [20]byte
	for i := 0; i < 20; i++ {
		var hi, lo byte
		if v, ok := hexNibble(s[i*2]); ok {
			hi = v
		} else {
			t.Fatalf("non-hex character in v2 hash: %q", s)
		}
		if v, ok := hexNibble(s[i*2+1]); ok {
			lo = v
		} else {
			t.Fatalf("non-hex character in v2 hash: %q", s)
		}
		out[i] = hi<<4 | lo
	}
	return out
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
