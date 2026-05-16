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
	"sort"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

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

// listV2Generations returns sorted seqs of chain.v2.<seq>.toml files in
// snapDir.
func listV2Generations(t *testing.T, snapDir string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	var seqs []uint64
	for _, e := range entries {
		if seq, ok := ParseChainTomlV2FileName(e.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

// TestRollingV2Publisher_GenerationsAdvance covers the basic shape:
// repeated Publish calls produce sequentially-numbered files and
// distinct infohashes (since the metainfo Name field changes each
// time).
func TestRollingV2Publisher_GenerationsAdvance(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)
	require.Empty(t, pub.History())

	inv := rollingTestInventory(t, 0x10)

	hashes := make([]metainfo.Hash, 5)
	for i := range hashes {
		h, err := pub.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err, "publish %d", i)
		require.NotEqual(t, metainfo.Hash{}, h, "publish %d: zero hash", i)
		hashes[i] = h
	}

	require.Equal(t, []uint64{0, 1, 2, 3, 4}, pub.History())
	require.Equal(t, []uint64{0, 1, 2, 3, 4}, listV2Generations(t, snapDir))
	for i := 1; i < len(hashes); i++ {
		require.NotEqual(t, hashes[i-1], hashes[i],
			"publish %d shares infohash with %d — Name field must differ", i, i-1)
	}
}

// TestRollingV2Publisher_RollingBufferEvicts caps history at the
// configured max and removes files for evicted generations.
func TestRollingV2Publisher_RollingBufferEvicts(t *testing.T) {
	snapDir := t.TempDir()
	const cap = 3
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, cap)
	require.NoError(t, err)

	inv := rollingTestInventory(t, 0x20)
	for i := 0; i < 7; i++ {
		_, err := pub.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err, "publish %d", i)
	}

	// History contains only the most recent `cap` generations.
	require.Equal(t, []uint64{4, 5, 6}, pub.History())

	// On disk: only those generations remain. Older files (and their
	// .torrent sidecars) are gone.
	require.Equal(t, []uint64{4, 5, 6}, listV2Generations(t, snapDir))
	for _, oldSeq := range []uint64{0, 1, 2, 3} {
		oldName := ChainTomlV2FileNameForSeq(oldSeq)
		_, err := os.Stat(filepath.Join(snapDir, oldName))
		require.True(t, os.IsNotExist(err), "%s should have been evicted", oldName)
		_, err = os.Stat(filepath.Join(snapDir, oldName+".torrent"))
		require.True(t, os.IsNotExist(err), "%s.torrent should have been evicted", oldName)
	}
}

// TestRollingV2Publisher_RestartResumesSeq covers the discovery path:
// a fresh publisher built against a snapDir that already contains
// generations resumes seq numbering from max(existing)+1.
func TestRollingV2Publisher_RestartResumesSeq(t *testing.T) {
	snapDir := t.TempDir()
	inv := rollingTestInventory(t, 0x30)

	// Publish a few generations, then drop the publisher.
	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := first.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err)
	}
	require.Equal(t, []uint64{0, 1, 2}, first.History())

	// New publisher discovers existing files, resumes from seq 3.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2}, second.History())

	_, err = second.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, second.History())
	require.Equal(t, []uint64{0, 1, 2, 3}, listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_RestartShrinksCap covers the case where a
// previous run wrote more generations than the new cap allows. The new
// publisher discovers all of them, then evicts on the next Publish.
func TestRollingV2Publisher_RestartShrinksCap(t *testing.T) {
	snapDir := t.TempDir()
	inv := rollingTestInventory(t, 0x40)

	// First run — cap of 10, publish 6 generations.
	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 10)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		_, err := first.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err)
	}
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5}, first.History())

	// Restart with a smaller cap — discovery sees all 6.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5}, second.History())

	// Next Publish triggers eviction down to 3 (counting the new entry).
	_, err = second.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{4, 5, 6}, second.History())
	require.Equal(t, []uint64{4, 5, 6}, listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_CleanupOrphans drops chain.v2.<seq>.toml files
// whose seq isn't in history — simulating a crash mid-publish that
// wrote files but never advanced the in-memory state.
func TestRollingV2Publisher_CleanupOrphans(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	inv := rollingTestInventory(t, 0x50)

	// Publish two generations normally.
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, pub.History())

	// Plant orphan files on disk (no .torrent sidecar, simulating a
	// crash partway through Publish).
	orphanName := ChainTomlV2FileNameForSeq(99)
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, orphanName), []byte("stub"), 0o644))

	// Cleanup removes the orphan, leaves history intact.
	require.NoError(t, pub.Cleanup())
	require.Equal(t, []uint64{0, 1}, pub.History())
	require.Equal(t, []uint64{0, 1}, listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_ENRUpdaterFires confirms enrUpdater is called
// once per Publish with the correct fields. Belt-and-braces against
// the auto-publisher integration regressing.
func TestRollingV2Publisher_ENRUpdaterFires(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

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

	// Nil updater: tolerated.
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
}

// TestParseChainTomlV2FileName covers the predicate's accept / reject
// behaviour. Lock the wire format down so a typo elsewhere can't widen it.
func TestParseChainTomlV2FileName(t *testing.T) {
	cases := []struct {
		name    string
		wantOK  bool
		wantSeq uint64
	}{
		{"chain.v2.0.toml", true, 0},
		{"chain.v2.42.toml", true, 42},
		{"chain.v2.18446744073709551614.toml", true, 18446744073709551614},
		// Reject everything else — no leading dot, no v1, no missing seq, etc.
		{"chain.v2.toml", false, 0},
		{"chain.v2.0", false, 0},
		{"chain.v2.0.tml", false, 0},
		{"chain.v3.0.toml", false, 0},
		{"chain.v2.-1.toml", false, 0},
		{"chain.v2.0.toml.bak", false, 0},
		{"prefix.chain.v2.0.toml", false, 0},
		{"chain.v2.0.toml.torrent", false, 0},
	}
	for _, c := range cases {
		seq, ok := ParseChainTomlV2FileName(c.name)
		require.Equal(t, c.wantOK, ok, "ok mismatch for %q", c.name)
		if c.wantOK {
			require.Equal(t, c.wantSeq, seq, "seq mismatch for %q", c.name)
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

	pub, err := NewRollingV2Publisher(snapDir, torrentFS, nil, 0)
	require.NoError(t, err)
	hash, err := pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, [20]byte{}, hash)

	gen0 := ChainTomlV2FileNameForSeq(0)
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
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	// selfCheck is nil — Publish proceeds normally.
	hash, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x20), 0, nil)
	require.NoError(t, err, "with nil selfCheck, Publish must succeed unchanged")
	require.NotEqual(t, metainfo.Hash{}, hash)

	// File was written.
	require.FileExists(t, filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
}

// TestRollingV2Publisher_SelfCheck_PassPropagates pins that when the
// self-check returns nil, the publish completes normally — file
// written, history advanced.
func TestRollingV2Publisher_SelfCheck_PassPropagates(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

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
	require.FileExists(t, filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
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
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	sentinel := errors.New("retire bug: hash mismatch on v1.0-headers.seg")
	pub.SetSelfCheck(func(m *ChainTomlV2) error {
		return sentinel
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x22), 0, nil)
	require.ErrorIs(t, err, sentinel,
		"self-check error propagates unchanged so callers can discriminate via errors.Is")

	// No file written — the failed generation must not appear on disk.
	require.NoFileExists(t, filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)),
		"chain.v2.0.toml must NOT exist after self-check failure — failed generations don't pollute disk")
	require.Empty(t, pub.History(),
		"history not advanced — next Publish() retries with seq=0, not seq=1")
}

// TestRollingV2Publisher_SelfCheck_RetryAfterFailure pins recovery
// behaviour: if the first Publish fails self-check and the next one
// passes, the next one writes seq=0 (the failed generation was
// never on disk, so no seq was consumed).
func TestRollingV2Publisher_SelfCheck_RetryAfterFailure(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	fail := errors.New("first attempt fails")
	pub.SetSelfCheck(func(m *ChainTomlV2) error { return fail })
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x23), 0, nil)
	require.ErrorIs(t, err, fail)

	// Now retire fixed itself — clear the self-check, retry.
	pub.SetSelfCheck(nil)
	hash, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x23), 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, metainfo.Hash{}, hash)

	require.Equal(t, []uint64{0}, pub.History(),
		"first successful publish uses seq=0 — the failed first attempt did not consume a seq")
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
		"v1.1-000000-001000-headers.seg":  "01",
		"v1.1-000000-001000-bodies.seg":   "02",
		"erigondb.toml":                   "ff",
		"salt-state.txt":                  "aa",
		"caplin/x.seg":                    "c1",
		"domain/v2.0-accounts.0-8192.kv":  "d1",
		"domain/v2.0-commitment.0-8192.kv": "d2",
	}, got, "every non-empty (name, hash) flattens to an item; empty-hash entries dropped")
}

// TestChainTomlV2ToItems_Nil pins defensive nil-input behaviour.
func TestChainTomlV2ToItems_Nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, ChainTomlV2ToItems(nil))
}

// TestRollingV2Publisher_Signer_NilDefault pins that with no signer
// configured, Publish proceeds unchanged and no .sig file is written.
func TestRollingV2Publisher_Signer_NilDefault(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x30), 0, nil)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
	require.NoFileExists(t, filepath.Join(snapDir, ChainV2SigFileNameForSeq(0)),
		"no signer configured → no .sig sidecar written")
}

// TestRollingV2Publisher_Signer_WritesSidecar pins that a configured
// signer produces a .sig sidecar next to the .toml file, carrying the
// returned bytes.
func TestRollingV2Publisher_Signer_WritesSidecar(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	var signedBytes []byte
	pub.SetSigner(func(data []byte) ([]byte, error) {
		signedBytes = data
		// 64-byte deterministic stub — same length as a real ECDSA sig.
		sig := make([]byte, 64)
		for i := range sig {
			sig[i] = byte(i)
		}
		return sig, nil
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x31), 0, nil)
	require.NoError(t, err)

	sigPath := filepath.Join(snapDir, ChainV2SigFileNameForSeq(0))
	require.FileExists(t, sigPath, ".sig sidecar must be written")

	// Verify .sig contains exactly what the signer returned.
	got, err := os.ReadFile(sigPath)
	require.NoError(t, err)
	require.Len(t, got, 64, "signature length matches what signer returned")

	// Verify the signer was called with the same bytes the .toml file
	// on disk contains — this is the MITM-defence invariant: signature
	// is over the EXACT bytes any receiver will see.
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
	require.NoError(t, err)
	require.Equal(t, tomlBytes, signedBytes,
		"signer must receive the same bytes that get persisted — consumer verifies signature over file bytes")
}

// TestRollingV2Publisher_Signer_FailureRollsBack pins that a signer
// error aborts the publish cleanly: the .toml file just written gets
// removed (don't seed unsigned content), no .sig is written, no
// history is advanced.
func TestRollingV2Publisher_Signer_FailureRollsBack(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	sentinel := errors.New("signing key unavailable")
	pub.SetSigner(func(data []byte) ([]byte, error) {
		return nil, sentinel
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x32), 0, nil)
	require.ErrorIs(t, err, sentinel)

	// .toml that was just written must be removed (don't seed unsigned).
	require.NoFileExists(t, filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)),
		"signer failure rolls back the .toml — must not leave unsigned content on disk")
	require.NoFileExists(t, filepath.Join(snapDir, ChainV2SigFileNameForSeq(0)))
	require.Empty(t, pub.History(),
		"signer failure does not advance history")
}

// TestRollingV2Publisher_Signer_EvictionRemovesSig pins that when
// the rolling buffer evicts a generation, the .sig sidecar is
// removed alongside the .toml — no orphaned signatures left on disk.
func TestRollingV2Publisher_Signer_EvictionRemovesSig(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 2)
	require.NoError(t, err)

	pub.SetSigner(func(data []byte) ([]byte, error) {
		sig := make([]byte, 64)
		return sig, nil
	})

	// 3 publishes with cap=2 → seq 0 is evicted, seq 1+2 retained.
	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), rollingTestInventory(t, byte(0x40+i)), 0, nil)
		require.NoError(t, err)
	}

	require.NoFileExists(t, filepath.Join(snapDir, ChainV2SigFileNameForSeq(0)),
		"evicted generation's .sig is removed alongside its .toml")
	require.FileExists(t, filepath.Join(snapDir, ChainV2SigFileNameForSeq(1)))
	require.FileExists(t, filepath.Join(snapDir, ChainV2SigFileNameForSeq(2)))
}
