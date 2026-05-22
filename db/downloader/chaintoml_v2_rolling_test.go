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

package downloader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// testENRFP is the deterministic 16-hex ENR fingerprint used by every
// publisher constructed in this file.
const testENRFP = "a1b2c3d4e5f60718"

// rollingTestInventory builds a tiny canonical inventory so the test
// runs in milliseconds. Two state files at canonical boundaries; their
// content is irrelevant for the publisher mechanics — what matters is
// that GenerateV2 produces non-empty output that round-trips.
func rollingTestInventory(_ *testing.T, marker byte) *snapshotinv.Inventory {
	inv := snapshotinv.NewInventory()
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      1024,
		Name:        "v1.0-accounts.0-1024.kv",
		TorrentHash: [20]byte{marker, 0xaa},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainStorage,
		FromStep:    0,
		ToStep:      1024,
		Name:        "v1.0-storage.0-1024.kv",
		TorrentHash: [20]byte{marker, 0xbb},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	return inv
}

// listV2Generations returns the genIDs of chain.v2.<genID>.toml files
// in snapDir (unordered — genID is opaque, not a counter).
func listV2Generations(t *testing.T, snapDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	var genIDs []string
	for _, e := range entries {
		if _, genID, ok := ParseChainTomlV2FileName(e.Name()); ok {
			genIDs = append(genIDs, genID)
		}
	}
	return genIDs
}

// TestRollingV2Publisher_GenerationsAdvance covers the basic shape:
// repeated Publish calls produce sequentially-numbered files and
// distinct infohashes (since the metainfo Name field changes each
// time).
func TestRollingV2Publisher_GenerationsAdvance(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)
	require.Empty(t, pub.History())

	hashes := make([]metainfo.Hash, 5)
	for i := range hashes {
		// Distinct inventory per publish: the generation ID is the
		// content hash, so identical inventory is one generation.
		h, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x10+byte(i)), 0, nil)
		require.NoError(t, err, "publish %d", i)
		require.NotEqual(t, metainfo.Hash{}, h, "publish %d: zero hash", i)
		hashes[i] = h
	}

	require.Len(t, pub.History(), 5)
	require.ElementsMatch(t, pub.History(), listV2Generations(t, snapDir))
	for i := 1; i < len(hashes); i++ {
		require.NotEqual(t, hashes[i-1], hashes[i],
			"publish %d shares infohash with %d — Name field must differ", i, i-1)
	}
}

// TestRollingV2Publisher_KeepsSubsetValidGenerations pins the "stable
// inventory" half of the validity rule: as long as every prior
// generation's name-set is ⊆ the latest, nothing gets evicted.
func TestRollingV2Publisher_KeepsSubsetValidGenerations(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	// Seven generations with distinct content but the same file-name
	// set — every prior generation stays a subset of the latest.
	for i := 0; i < 7; i++ {
		_, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x20+byte(i)), 0, nil)
		require.NoError(t, err, "publish %d", i)
	}

	// All 7 generations are subset-valid — none evicted.
	require.Len(t, pub.History(), 7)
	require.ElementsMatch(t, pub.History(), listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_EvictsWhenReferencesRetired pins the other
// half of the validity rule: the moment a merge retires a name from
// inventory, every prior generation that listed it becomes invalid
// and is removed from disk.
func TestRollingV2Publisher_EvictsWhenReferencesRetired(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	// gen 0,1: distinct content, both listing {accounts.0-1024,
	// storage.0-1024}.
	for i := 0; i < 2; i++ {
		_, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x21+byte(i)), 0, nil)
		require.NoError(t, err)
	}
	oldGenIDs := append([]string(nil), pub.History()...)
	require.Len(t, oldGenIDs, 2)

	// Simulate a merge: drop accounts.0-1024 and storage.0-1024,
	// add a single merged file. Re-publish.
	inv2 := snapshotinv.NewInventory()
	inv2.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x21, 0xcc},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	_, err = pub.Publish(context.Background(), inv2, 0, nil)
	require.NoError(t, err)

	// The two earlier generations listed accounts.0-1024 + storage.0-1024
	// — neither is in the merged generation's manifest, so both are
	// invalid and evicted.
	require.Len(t, pub.History(), 1)
	require.ElementsMatch(t, pub.History(), listV2Generations(t, snapDir))
	for _, oldGenID := range oldGenIDs {
		oldName := ChainTomlV2FileName(testENRFP, oldGenID)
		_, err := os.Stat(filepath.Join(snapDir, oldName))
		require.True(t, os.IsNotExist(err), "%s should have been evicted", oldName)
		_, err = os.Stat(filepath.Join(snapDir, oldName+".torrent"))
		require.True(t, os.IsNotExist(err), "%s.torrent should have been evicted", oldName)
	}
}

// TestRollingV2Publisher_RestartResumesGenerations covers the discovery
// path: a fresh publisher built against a snapDir that already contains
// generations recovers them all into its history, and a further Publish
// adds a new generation alongside.
func TestRollingV2Publisher_RestartResumesGenerations(t *testing.T) {
	snapDir := t.TempDir()

	// Publish a few distinct generations, then drop the publisher.
	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	first.SetENRFingerprint(testENRFP)
	for i := 0; i < 3; i++ {
		_, err := first.Publish(context.Background(), rollingTestInventory(t, 0x30+byte(i)), 0, nil)
		require.NoError(t, err)
	}
	firstGenIDs := first.History()
	require.Len(t, firstGenIDs, 3)

	// New publisher discovers the existing files into its history.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	second.SetENRFingerprint(testENRFP)
	require.ElementsMatch(t, firstGenIDs, second.History())

	_, err = second.Publish(context.Background(), rollingTestInventory(t, 0x33), 0, nil)
	require.NoError(t, err)
	require.Len(t, second.History(), 4)
	require.ElementsMatch(t, second.History(), listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_RestartRecoversNameSets covers the discovery
// path's validity behaviour: a fresh publisher built against a snapDir
// with existing generations parses each manifest so the validity rule
// is enforceable on the first post-restart Publish.
func TestRollingV2Publisher_RestartRecoversNameSets(t *testing.T) {
	snapDir := t.TempDir()

	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	first.SetENRFingerprint(testENRFP)
	for i := 0; i < 3; i++ {
		_, err := first.Publish(context.Background(), rollingTestInventory(t, 0x40+byte(i)), 0, nil)
		require.NoError(t, err)
	}
	require.Len(t, first.History(), 3)

	// Restart — discovery parses each chain.v2.*.toml and recovers
	// name-sets so the next Publish can evaluate validity.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	second.SetENRFingerprint(testENRFP)
	require.Len(t, second.History(), 3)

	// Publish with a retiring inventory — every prior gen must evict.
	inv2 := snapshotinv.NewInventory()
	inv2.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x40, 0xcc},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	_, err = second.Publish(context.Background(), inv2, 0, nil)
	require.NoError(t, err)
	require.Len(t, second.History(), 1)
	require.ElementsMatch(t, second.History(), listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_RestartRepublishIdenticalDedups covers the
// content-addressing restart interaction: after a restart the discovery
// scan rebuilds history in directory-scan order, NOT publish order.
// Re-publishing content identical to an already-discovered generation
// produces the same content-addressed genID — it must be recognised as
// the same generation and not appended a second time, regardless of
// where that generation sits in the scanned history.
func TestRollingV2Publisher_RestartRepublishIdenticalDedups(t *testing.T) {
	snapDir := t.TempDir()

	// Publish three distinct generations, remembering each inventory so
	// the exact same content can be republished after the restart.
	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	first.SetENRFingerprint(testENRFP)
	invs := []*snapshotinv.Inventory{
		rollingTestInventory(t, 0x70),
		rollingTestInventory(t, 0x71),
		rollingTestInventory(t, 0x72),
	}
	for i, inv := range invs {
		_, err := first.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err, "publish %d", i)
	}
	require.Len(t, first.History(), 3)

	// Restart: discovery rebuilds history in directory-scan (genID-sorted)
	// order — generally NOT the order they were published in.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	second.SetENRFingerprint(testENRFP)
	require.Len(t, second.History(), 3)

	// Re-publish each original inventory. Every one is byte-identical to
	// a generation already in history, so each is a no-op republish: the
	// history must stay exactly the three discovered generations. With a
	// last-entry-only dedup this fails the moment a republished
	// generation isn't the current tail — a duplicate genID is appended.
	for i, inv := range invs {
		_, err := second.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err, "republish %d", i)
		require.Len(t, second.History(), 3,
			"republish %d of identical content must not grow history", i)
		require.ElementsMatch(t, second.History(), listV2Generations(t, snapDir),
			"republish %d: history must stay in sync with on-disk generations", i)
	}
}

// TestRollingV2Publisher_ResumeSeeding_NilDownloaderNoop pins that
// ResumeSeeding is a clean no-op when no downloader is wired — the
// default for unit tests. Production wires a downloader; this only
// guards the defensive path.
func TestRollingV2Publisher_ResumeSeeding_NilDownloaderNoop(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x80), 0, nil)
	require.NoError(t, err)

	require.NoError(t, pub.ResumeSeeding(context.Background()),
		"ResumeSeeding with no downloader must be a clean no-op")
}

// TestRollingV2Publisher_CleanupOrphans drops chain.v2.<genID>.toml
// files whose genID isn't in history — simulating a crash mid-publish
// that wrote files but never advanced the in-memory state.
func TestRollingV2Publisher_CleanupOrphans(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	// Publish two distinct generations normally.
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x50), 0, nil)
	require.NoError(t, err)
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x51), 0, nil)
	require.NoError(t, err)
	require.Len(t, pub.History(), 2)

	// Plant an orphan file on disk under a valid-shape genID that no
	// generation used (no .torrent sidecar, simulating a crash partway
	// through Publish).
	orphanName := ChainTomlV2FileName(testENRFP, "ffffffffffffffff")
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, orphanName), []byte("stub"), 0o644))

	// Cleanup removes the orphan, leaves history intact.
	require.NoError(t, pub.Cleanup())
	require.Len(t, pub.History(), 2)
	require.ElementsMatch(t, pub.History(), listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_ENRUpdaterFires confirms enrUpdater is called
// once per Publish with the correct fields. Belt-and-braces against
// the auto-publisher integration regressing.
func TestRollingV2Publisher_ENRUpdaterFires(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	inv := rollingTestInventory(t, 0x60)

	var calls int
	var lastCT enr.ChainToml
	enrUpdater := func(ct enr.ChainToml) {
		calls++
		lastCT = ct
	}
	hash, err := pub.Publish(context.Background(), inv, 12345, enrUpdater)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, [20]byte(hash), lastCT.InfoHash)
	require.Equal(t, uint64(12345), lastCT.AuthoritativeBlocks)
	require.Equal(t, uint64(12345), lastCT.KnownBlocks)
	require.Equal(t, [20]byte{}, lastCT.ContentUCANHash,
		"no content minter wired → ENR carries no Content UCAN hash")

	// Nil updater: tolerated.
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
}

// TestRollingV2Publisher_ENRUpdaterStampsContentUCANHash confirms that
// when a content minter IS wired, Publish builds the Content UCAN
// torrent and stamps its non-zero info-hash into the ENR — the
// counterpart to ENRUpdaterFires, which only covers the no-minter case.
func TestRollingV2Publisher_ENRUpdaterStampsContentUCANHash(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	var mintedOver []byte
	pub.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
		mintedOver = tomlBytes
		return []byte("content-ucan-attestation-bytes"), nil
	})

	inv := rollingTestInventory(t, 0x70)

	var lastCT enr.ChainToml
	hash, err := pub.Publish(context.Background(), inv, 12345, func(ct enr.ChainToml) {
		lastCT = ct
	})
	require.NoError(t, err)
	require.Equal(t, [20]byte(hash), lastCT.InfoHash)
	require.NotEqual(t, [20]byte{}, lastCT.ContentUCANHash,
		"content minter wired → ENR carries the Content UCAN info-hash")
	require.NotEmpty(t, mintedOver, "minter receives the manifest TOML bytes")
}

// TestParseChainTomlV2FileName covers the predicate's accept / reject
// behaviour. Lock the wire format down so a typo elsewhere can't widen it.
func TestParseChainTomlV2FileName(t *testing.T) {
	cases := []struct {
		name      string
		wantOK    bool
		wantENRFP string
		wantGenID string
	}{
		{"chain.v2.a1b2c3d4e5f60718.00000000000000ff.toml", true, "a1b2c3d4e5f60718", "00000000000000ff"},
		{"chain.v2.0123456789abcdef.fedcba9876543210.toml", true, "0123456789abcdef", "fedcba9876543210"},
		// Reject everything else — wrong-length fingerprint or genID,
		// non-hex, no leading dot, no v1, missing genID, etc.
		{"chain.v2.a1b2c3d4e5f60718.0.toml", false, "", ""},  // genID not 16 hex
		{"chain.v2.a1b2c3d4e5f60718.42.toml", false, "", ""}, // genID not 16 hex
		{"chain.v2.0.toml", false, "", ""},
		{"chain.v2.toml", false, "", ""},
		{"chain.v2.a1b2c3d4e5f60718.00000000000000ff", false, "", ""},
		{"chain.v2.a1b2c3d4e5f60718.00000000000000ff.tml", false, "", ""},
		{"chain.v3.a1b2c3d4e5f60718.00000000000000ff.toml", false, "", ""},
		{"chain.v2.a1b2c3d4e5f6071.00000000000000ff.toml", false, "", ""},   // 15-hex fingerprint
		{"chain.v2.a1b2c3d4e5f607189.00000000000000ff.toml", false, "", ""}, // 17-hex fingerprint
		{"chain.v2.a1b2c3d4e5f60718.00000000000000fg.toml", false, "", ""},  // non-hex genID
		{"chain.v2.A1B2C3D4E5F60718.00000000000000ff.toml", false, "", ""},  // uppercase rejected
		{"chain.v2.a1b2c3d4e5f60718.00000000000000ff.toml.bak", false, "", ""},
		{"prefix.chain.v2.a1b2c3d4e5f60718.00000000000000ff.toml", false, "", ""},
		{"chain.v2.a1b2c3d4e5f60718.00000000000000ff.toml.torrent", false, "", ""},
	}
	for _, c := range cases {
		enrFP, genID, ok := ParseChainTomlV2FileName(c.name)
		require.Equal(t, c.wantOK, ok, "ok mismatch for %q", c.name)
		if c.wantOK {
			require.Equal(t, c.wantENRFP, enrFP, "enrFP mismatch for %q", c.name)
			require.Equal(t, c.wantGenID, genID, "genID mismatch for %q", c.name)
		}
	}
}

// TestPublishV2NonEmptyFromDiskTorrents is the regression guard for the
// gap-D2 failure mode. In production the storage component never feeds
// torrent hashes back into the inventory, so inventory FileEntries carry
// a zero TorrentHash even when the files — and their .torrent metainfo —
// are on disk. GenerateV2 skips zero-hash entries, so without the
// disk-scan backfill (populateInventoryTorrentHashes, run inside
// RollingV2Publisher.Publish) the publisher emits an empty "version = 2"
// manifest that silently starves every V2 consumer. This test builds
// that exact setup and asserts the published manifest is non-empty and
// the inventory itself gets backfilled.
func TestPublishV2NonEmptyFromDiskTorrents(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)

	relFiles := []string{
		"domain/v1.0-accounts.0-1024.kv",
		"domain/v1.0-storage.0-1024.kv",
	}
	for i, rel := range relFiles {
		p := filepath.Join(snapDir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte{'s', 'n', 'a', 'p', byte(i)}, 0o644))
		ok, err := BuildTorrentIfNeed(context.Background(), rel, snapDir, torrentFS)
		require.NoError(t, err)
		require.True(t, ok, "%s.torrent must be built", rel)
	}

	inv := snapshotinv.NewInventory()
	inv.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainAccounts, FromStep: 0, ToStep: 1024,
		Name: "v1.0-accounts.0-1024.kv", Local: true, Trust: snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain: snapshotinv.DomainStorage, FromStep: 0, ToStep: 1024,
		Name: "v1.0-storage.0-1024.kv", Local: true, Trust: snapshotinv.TrustVerified,
	})

	// Precondition: a zero-hash inventory yields an empty manifest — the bug.
	require.Empty(t, GenerateV2(inv).Domains,
		"precondition: GenerateV2 on a zero-hash inventory must be empty")

	pub, err := NewRollingV2Publisher(snapDir, torrentFS, nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)
	hash, err := pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, [20]byte{}, hash)

	gen0 := ChainTomlV2FileName(testENRFP, pub.History()[0])
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, gen0))
	require.NoError(t, err)
	m, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(m.Domains), 2,
		"published manifest must list the on-disk domain files, got %d domains", len(m.Domains))

	// The inventory entries were backfilled with the on-disk hashes.
	for _, e := range inv.LocalFiles(snapshotinv.DomainAccounts) {
		require.NotEqual(t, [20]byte{}, e.TorrentHash,
			"inventory entry %s should have its hash backfilled from disk", e.Name)
	}
}

// TestRollingV2Publisher_SelfCheck_NilDefault pins the test-friendly
// default: with no self-check configured, Publish proceeds as before.
// Tests/harness that don't wire a self-check must not be affected.
func TestRollingV2Publisher_SelfCheck_NilDefault(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	// selfCheck is nil — Publish proceeds normally.
	hash, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x20), 0, nil)
	require.NoError(t, err, "with nil selfCheck, Publish must succeed unchanged")
	require.NotEqual(t, metainfo.Hash{}, hash)

	// File was written.
	require.FileExists(t, filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, pub.History()[0])))
}

// TestRollingV2Publisher_SelfCheck_PassPropagates pins that when the
// self-check returns nil, the publish completes normally — file
// written, history advanced.
func TestRollingV2Publisher_SelfCheck_PassPropagates(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	called := false
	pub.SetSelfCheck(func(m *ChainTomlV2) error {
		called = true
		require.NotNil(t, m, "self-check receives the generated manifest")
		return nil // pass
	})

	hash, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x21), 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, metainfo.Hash{}, hash)
	require.True(t, called, "self-check callback was invoked")
	require.FileExists(t, filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, pub.History()[0])))
}

// TestRollingV2Publisher_SelfCheck_FailLoud is the core invariant: a
// self-check error aborts the publish completely. NO file is
// written, NO torrent built, NO history advanced. Per direction
// (docs/plans/20260515-three-layer-snapshot-distribution.md "fail
// loud"): the error propagates to the caller, which logs it at Warn
// and the node continues running — but this generation is skipped
// cleanly without polluting on-disk state.
func TestRollingV2Publisher_SelfCheck_FailLoud(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	sentinel := errors.New("retire bug: hash mismatch on v1.0-headers.seg")
	pub.SetSelfCheck(func(m *ChainTomlV2) error {
		return sentinel
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x22), 0, nil)
	require.ErrorIs(t, err, sentinel,
		"self-check error propagates unchanged so callers can discriminate via errors.Is")

	// No file written — the failed generation must not appear on disk.
	require.Empty(t, listV2Generations(t, snapDir),
		"no chain.v2.*.toml must exist after self-check failure — failed generations don't pollute disk")
	require.Empty(t, pub.History(),
		"history not advanced — the failed generation consumed nothing")
}

// TestRollingV2Publisher_SelfCheck_RetryAfterFailure pins recovery
// behaviour: if the first Publish fails self-check and the next one
// passes, the next one writes seq=0 (the failed generation was
// never on disk, so no seq was consumed).
func TestRollingV2Publisher_SelfCheck_RetryAfterFailure(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	fail := errors.New("first attempt fails")
	pub.SetSelfCheck(func(m *ChainTomlV2) error { return fail })
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x23), 0, nil)
	require.ErrorIs(t, err, fail)

	// Now retire fixed itself — clear the self-check, retry.
	pub.SetSelfCheck(nil)
	hash, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x23), 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, metainfo.Hash{}, hash)

	require.Len(t, pub.History(), 1,
		"only the successful publish is in history — the failed first attempt consumed nothing")
}

// TestChainTomlV2ToItems_Flatten pins the flatten helper that
// external wiring code uses to feed CheckOwnAdvertisement. Every
// non-empty (name, hash) across Blocks + Meta + Salt + Caplin + all
// Domains shows up in the result; empty-hash entries are skipped.
func TestChainTomlV2ToItems_Flatten(t *testing.T) {
	t.Parallel()

	m := &ChainTomlV2{
		Version: 2,
		Blocks: map[string]string{
			"v1.1-000000-001000-headers.seg": "01",
			"v1.1-000000-001000-bodies.seg":  "02",
			"empty.seg":                      "", // skipped
		},
		Meta:   map[string]string{"erigondb.toml": "ff"},
		Salt:   map[string]string{"salt-state.txt": "aa"},
		Caplin: []CaplinFileEntry{{Name: "caplin/x.seg", Hash: "c1"}, {Name: "caplin/empty.seg", Hash: ""}},
		Domains: map[string]*DomainManifest{
			"accounts": {Files: []DomainFileEntry{
				{Name: "domain/v2.0-accounts.0-8192.kv", Hash: "d1"},
				{Name: "domain/v2.0-accounts.0-8192.kvi", Hash: ""}, // skipped
			}},
			"commitment": {Files: []DomainFileEntry{
				{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "d2"},
			}},
		},
	}

	items := ChainTomlV2ToItems(m)
	got := make(map[string]string, len(items))
	for _, it := range items {
		got[it.Name] = it.Hash
	}

	require.Equal(t, map[string]string{
		"v1.1-000000-001000-headers.seg":   "01",
		"v1.1-000000-001000-bodies.seg":    "02",
		"erigondb.toml":                    "ff",
		"salt-state.txt":                   "aa",
		"caplin/x.seg":                     "c1",
		"domain/v2.0-accounts.0-8192.kv":   "d1",
		"domain/v2.0-commitment.0-8192.kv": "d2",
	}, got, "every non-empty (name, hash) flattens to an item; empty-hash entries dropped")
}

// TestChainTomlV2ToItems_Nil pins defensive nil-input behaviour.
func TestChainTomlV2ToItems_Nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, ChainTomlV2ToItems(nil))
}

// TestRollingV2Publisher_ContentUCAN_NilDefault pins that with no
// content minter configured, Publish proceeds unchanged and no .ucan
// sidecar is written.
func TestRollingV2Publisher_ContentUCAN_NilDefault(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x30), 0, nil)
	require.NoError(t, err)
	genID := pub.History()[0]
	require.FileExists(t, filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, genID)))
	require.NoFileExists(t, filepath.Join(snapDir, ChainV2ContentUCANFileName(testENRFP, genID)),
		"no content minter configured → no .ucan sidecar written")
}

// TestRollingV2Publisher_ContentUCAN_WritesSidecar pins that a
// configured content minter produces a .ucan sidecar next to the
// .toml file, carrying the returned bytes, and is called with the
// exact bytes the .toml file on disk holds.
func TestRollingV2Publisher_ContentUCAN_WritesSidecar(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	var mintedOver []byte
	ucan := []byte("content-ucan-cbor-stub")
	pub.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
		mintedOver = tomlBytes
		return ucan, nil
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x31), 0, nil)
	require.NoError(t, err)
	genID := pub.History()[0]

	ucanPath := filepath.Join(snapDir, ChainV2ContentUCANFileName(testENRFP, genID))
	require.FileExists(t, ucanPath, ".ucan sidecar must be written")

	// Verify .ucan contains exactly what the minter returned.
	got, err := os.ReadFile(ucanPath)
	require.NoError(t, err)
	require.Equal(t, ucan, got, ".ucan contains exactly what the minter returned")

	// Verify the minter was called with the same bytes the .toml file
	// on disk contains — the Content UCAN binds chain.v2:hash:<sha256>
	// over the EXACT bytes any receiver will see.
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, genID)))
	require.NoError(t, err)
	require.Equal(t, tomlBytes, mintedOver,
		"minter must receive the same bytes that get persisted — consumer verifies the attestation over file bytes")
}

// TestRollingV2Publisher_ContentUCAN_FailureRollsBack pins that a
// minter error aborts the publish cleanly: the .toml file just
// written gets removed (don't seed unattested content), no .ucan is
// written, no history is advanced.
func TestRollingV2Publisher_ContentUCAN_FailureRollsBack(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	sentinel := errors.New("operator key unavailable")
	pub.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
		return nil, sentinel
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x32), 0, nil)
	require.ErrorIs(t, err, sentinel)

	// The .toml + .ucan just written must be removed (don't seed
	// unattested content) — no manifest or UCAN of any genID survives.
	require.Empty(t, listV2Generations(t, snapDir),
		"minter failure rolls back the .toml — must not leave unattested content on disk")
	ucans, err := filepath.Glob(filepath.Join(snapDir, "*.ucan"))
	require.NoError(t, err)
	require.Empty(t, ucans, "minter failure must not leave a .ucan on disk")
	require.Empty(t, pub.History(),
		"minter failure does not advance history")
}

// TestRollingV2Publisher_ContentUCAN_EvictionRemovesUCAN pins that
// when the rolling buffer evicts a generation, its Content UCAN
// sidecar is removed alongside the .toml — no orphaned attestations
// left on disk.
func TestRollingV2Publisher_ContentUCAN_EvictionRemovesUCAN(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	pub.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
		return []byte("u"), nil
	})

	// gen 0: lists accounts.0-1024 + storage.0-1024.
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x60), 0, nil)
	require.NoError(t, err)
	gen0ID := pub.History()[0]

	// gen 1: retires both names — gen 0 is now invalid.
	inv2 := snapshotinv.NewInventory()
	inv2.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x60, 0xcc},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	_, err = pub.Publish(context.Background(), inv2, 0, nil)
	require.NoError(t, err)

	// gen 0 evicted; only the surviving generation remains in history.
	require.Len(t, pub.History(), 1)
	survivorID := pub.History()[0]
	require.NoFileExists(t, filepath.Join(snapDir, ChainV2ContentUCANFileName(testENRFP, gen0ID)),
		"evicted generation's Content UCAN is removed alongside its .toml")
	require.FileExists(t, filepath.Join(snapDir, ChainV2ContentUCANFileName(testENRFP, survivorID)))
}
