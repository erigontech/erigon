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

// Real-content validation: run the swarm-distribution lifecycle against
// real Erigon snapshot files on disk rather than synthetic placeholder
// bytes. The goal is to prove the transport mechanism (real
// anacrolix/torrent + real sentry + real flow orchestrator) handles
// production-shaped content — gigabyte-scale files, real piece hashes,
// real on-disk layouts — so any remaining unknowns in the integration
// session are storage-layer, not transport-layer.
//
// The test is driven by the SNAPSHOT_FIXTURE_DIR env var pointing at a
// directory of real snapshot files (v1.0-*.kv, *.kvi, *.seg, etc.). If
// unset, the test skips — CI runs don't need to fail for lack of real
// data.
//
// Invocation:
//
//	SNAPSHOT_FIXTURE_DIR=/path/to/real/snapshots \
//	go test -tags=p2p_integration -timeout 4h -v \
//	  -run TestP2P_Swarm_RealContent \
//	  ./node/components/integration/snapshot/scenarios/

package scenarios_test

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

// realFile describes one real on-disk snapshot file picked up from the
// fixture directory.
type realFile struct {
	name     string // filename only (no dir prefix)
	path     string // absolute path on disk
	size     int64
	domain   snapshot.Domain // empty for block files
	fromStep uint64
	toStep   uint64
}

// stateDomainNameRE matches v1.0-<domain>.<from>-<to>.{kv,kvi}
var stateDomainNameRE = regexp.MustCompile(`^v\d+\.\d+-([a-z]+)\.(\d+)-(\d+)\.(kv|kvi)$`)

// blockFileNameRE matches v1.0-<from>-<to>-<role>.{seg,idx}
var blockFileNameRE = regexp.MustCompile(`^v\d+\.\d+-(\d+)-(\d+)-[a-z]+\.(seg|idx)$`)

// scanFixtureDir walks dir for real snapshot files we can use as
// fixtures. Recognises state-domain and block-file naming. Skips files
// that don't match (e.g. metadata, salts, torrents).
func scanFixtureDir(t *testing.T, dir string) []realFile {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	knownStateDomains := map[string]snapshot.Domain{
		"accounts":   snapshot.DomainAccounts,
		"storage":    snapshot.DomainStorage,
		"code":       snapshot.DomainCode,
		"commitment": snapshot.DomainCommitment,
	}

	var out []realFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()

		if m := stateDomainNameRE.FindStringSubmatch(name); m != nil {
			domain, ok := knownStateDomains[m[1]]
			if !ok {
				continue
			}
			from, _ := strconv.ParseUint(m[2], 10, 64)
			to, _ := strconv.ParseUint(m[3], 10, 64)
			if !isPowerOf2Range(from, to) {
				// GenerateV2's canonicity filter drops non-canonical
				// entries; if we can't advertise it, don't fixture it.
				continue
			}
			info, err := e.Info()
			require.NoError(t, err)
			out = append(out, realFile{
				name:     name,
				path:     filepath.Join(dir, name),
				size:     info.Size(),
				domain:   domain,
				fromStep: from,
				toStep:   to,
			})
			continue
		}

		if m := blockFileNameRE.FindStringSubmatch(name); m != nil {
			// Only .seg; .idx files accompany them but aren't torrented
			// as primary payload for the snapshot-flow test.
			if !strings.HasSuffix(name, ".seg") {
				continue
			}
			from, _ := strconv.ParseUint(m[1], 10, 64)
			to, _ := strconv.ParseUint(m[2], 10, 64)
			info, err := e.Info()
			require.NoError(t, err)
			out = append(out, realFile{
				name:     name,
				path:     filepath.Join(dir, name),
				size:     info.Size(),
				fromStep: from,
				toStep:   to,
			})
		}
	}
	// Deterministic ordering for reproducible partition assignment.
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func isPowerOf2Range(from, to uint64) bool {
	if to <= from {
		return false
	}
	size := to - from
	return size&(size-1) == 0 && from%size == 0
}

// TestP2P_Swarm_RealContent runs the full real-stack distribution
// lifecycle against real Erigon snapshot files. Partitions them across
// four seeders round-robin, leaves each seeder with a disjoint subset,
// brings up a fresh leecher, and verifies every file on the leecher is
// byte-identical to its source.
//
// Skips unless SNAPSHOT_FIXTURE_DIR is set. Timeout budget scales with
// content size — pass a generous -timeout on the go test invocation.
func TestP2P_Swarm_RealContent(t *testing.T) {
	dir := os.Getenv("SNAPSHOT_FIXTURE_DIR")
	if dir == "" {
		t.Skip("SNAPSHOT_FIXTURE_DIR not set; skipping real-content test")
	}

	files := scanFixtureDir(t, dir)
	if len(files) == 0 {
		t.Skipf("no recognised snapshot files in %s — skipping", dir)
	}

	// Sanity: at least one state file in the fixture. The phased
	// scheduling code path only gets exercised if there's state to
	// download; pure-blocks fixtures still test the block distribution
	// but skip the state gate.
	var haveState, haveBlocks bool
	var totalBytes int64
	for _, f := range files {
		if f.domain != "" {
			haveState = true
		} else {
			haveBlocks = true
		}
		totalBytes += f.size
	}
	t.Logf("fixture dir: %s", dir)
	t.Logf("fixture files: %d total, haveState=%v haveBlocks=%v", len(files), haveState, haveBlocks)
	t.Logf("total bytes: %.1f MiB (%.2f GiB)",
		float64(totalBytes)/(1<<20), float64(totalBytes)/(1<<30))

	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	// Four seeders, round-robin partition.
	const numSeeders = 4
	seeders := make([]*harness.P2PNode, numSeeders)
	for i := range seeders {
		seeders[i] = harness.NewP2PNode(t, logger)
	}

	// Seed each file on its assigned seeder. SeedFile reads the bytes
	// into memory, computes the torrent hash via BuildTorrentIfNeed,
	// and adds to the seeder's inventory. For very large files this
	// dominates setup time — expected.
	t.Logf("seeding %d files across %d peers...", len(files), numSeeders)
	setupStart := time.Now()
	expectedHashes := make(map[string][20]byte, len(files))
	expectedContentHash := make(map[string][32]byte, len(files))
	for i, f := range files {
		content, err := os.ReadFile(f.path)
		require.NoError(t, err, "reading %s", f.path)
		expectedContentHash[f.name] = sha256.Sum256(content)
		seederIdx := i % numSeeders
		h := seeders[seederIdx].SeedFile(f.name, content, f.domain, f.fromStep, f.toStep)
		expectedHashes[f.name] = h
	}
	t.Logf("seeding complete in %.2fs", time.Since(setupStart).Seconds())

	// Every seeder publishes its V2 manifest + ENR.
	for _, s := range seeders {
		v2 := s.PublishV2Manifest()
		_, btPort := s.LocalTorrentAddr()
		s.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2})
		s.SetDevP2PENREntry(enr.BT(btPort))
	}

	// Fresh leecher joins. Connect to all seeders at both layers.
	leecher := harness.NewP2PNode(t, logger)
	for _, s := range seeders {
		leecher.AddSeederPeer(s)
	}

	var (
		manifestCount atomic.Int32
		promotedCount atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	t.Logf("starting real-content transfer...")
	transferStart := time.Now()
	for _, s := range seeders {
		leecher.AddDevP2PPeer(s.DevP2PSelf())
	}

	expectedTotal := int32(len(files))
	// Big budget: allows ~30 MiB/s sustained which is conservative for
	// loopback even with piece hashing overhead.
	timeout := time.Duration(totalBytes)/30 + 60*time.Second
	if timeout < 5*time.Minute {
		timeout = 5 * time.Minute
	}
	waitForP2P(t, func() bool {
		return promotedCount.Load() >= expectedTotal &&
			leecher.Orch.PendingCount() == 0
	}, timeout, "complete real archive promoted on leecher")
	elapsed := time.Since(transferStart)

	t.Logf("transferred %.1f MiB in %.2fs → %.1f MiB/s",
		float64(totalBytes)/(1<<20), elapsed.Seconds(),
		float64(totalBytes)/elapsed.Seconds()/(1<<20))
	t.Logf("manifests=%d promoted=%d", manifestCount.Load(), promotedCount.Load())

	// Byte-equality on every file. This is the only assertion that
	// proves the transport moved real content correctly.
	t.Logf("verifying byte-equality on %d files...", len(files))
	verifyStart := time.Now()
	for _, f := range files {
		gotPath := filepath.Join(leecher.Dirs.Snap, f.name)
		got, err := os.ReadFile(gotPath)
		require.NoError(t, err, "leecher missing real file %s", f.name)
		require.Equal(t, len(got), int(f.size),
			"size mismatch for %s: want %d got %d", f.name, f.size, len(got))
		gotHash := sha256.Sum256(got)
		require.Equal(t, expectedContentHash[f.name], gotHash,
			"content hash mismatch for %s", f.name)
	}
	t.Logf("byte-equality confirmed on %d files in %.2fs",
		len(files), time.Since(verifyStart).Seconds())

	// Inventory torrent hashes match the seeder-side hashes for every file.
	for _, f := range files {
		var entry *snapshot.FileEntry
		if f.domain != "" {
			for _, e := range leecher.Inventory.LocalFiles(f.domain) {
				if e.Name == f.name {
					entry = e
					break
				}
			}
		} else {
			for _, e := range leecher.Inventory.BlockFiles() {
				if e.Name == f.name && e.Local {
					entry = e
					break
				}
			}
		}
		require.NotNil(t, entry, "inventory missing entry for %s", f.name)
		require.Equal(t, expectedHashes[f.name], entry.TorrentHash,
			"inventory torrent hash mismatch for %s", f.name)
		require.Equal(t, snapshot.TrustVerified, entry.Trust,
			"inventory trust level for %s", f.name)
	}
}
