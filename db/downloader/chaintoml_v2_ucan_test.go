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
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
)

// listAuthorityUCANRevs returns the content-addressed <rev>s of
// chain.ucan.authority.<fp>.<rev>.bin files in snapDir (unordered).
func listAuthorityUCANRevs(t *testing.T, snapDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	var revs []string
	for _, e := range entries {
		if _, rev, ok := ParseChainAuthorityUCANFileName(e.Name()); ok {
			revs = append(revs, rev)
		}
	}
	return revs
}

// TestRollingV2Publisher_NoDelegationSourceNoUCAN confirms the default
// publisher (no delegation source set) writes V2 only — no UCAN sidecar
// files, and the V2 manifest's AuthorityUCANHash is empty.
func TestRollingV2Publisher_NoDelegationSourceNoUCAN(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x10), 0, nil)
	require.NoError(t, err)

	require.Len(t, listV2Generations(t, snapDir), 1)
	require.Empty(t, listAuthorityUCANRevs(t, snapDir),
		"no delegation source → no Authority UCAN sidecar")

	// The on-disk V2 manifest must carry an empty AuthorityUCANHash field.
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, pub.History()[0])))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.Empty(t, manifest.AuthorityUCANHash)
}

// TestRollingV2Publisher_DelegationSourceWritesPair confirms that with
// a delegation source configured, Publish() writes the content-addressed
// Authority UCAN sidecar and the V2's AuthorityUCANHash matches the UCAN
// torrent's infohash.
func TestRollingV2Publisher_DelegationSourceWritesPair(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	ucanBytes := []byte("delegation-payload-v1")
	pub.SetDelegationSource(func() ([]byte, error) { return ucanBytes, nil })

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x10), 0, nil)
	require.NoError(t, err)
	genID := pub.History()[0]

	rev := genIDFromContent(ucanBytes)
	ucanName := ChainAuthorityUCANFileName(testENRFP, rev)

	require.Len(t, listV2Generations(t, snapDir), 1)
	require.ElementsMatch(t, []string{rev}, listAuthorityUCANRevs(t, snapDir))

	// UCAN file content matches what the source returned.
	gotUCAN, err := os.ReadFile(filepath.Join(snapDir, ucanName))
	require.NoError(t, err)
	require.Equal(t, ucanBytes, gotUCAN)

	// UCAN .torrent exists alongside.
	_, err = os.Stat(filepath.Join(snapDir, ucanName+".torrent"))
	require.NoError(t, err)

	// V2 manifest's AuthorityUCANHash matches the UCAN torrent's infohash.
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, genID)))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.NotEmpty(t, manifest.AuthorityUCANHash)

	tf := NewAtomicTorrentFS(snapDir)
	ucanSpec, err := tf.LoadByName(ucanName + ".torrent")
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ucanSpec.InfoHash[:]), manifest.AuthorityUCANHash,
		"V2.AuthorityUCANHash must equal hex(UCAN torrent infohash)")
}

// TestRollingV2Publisher_RotatingDelegation confirms the source is
// consulted on every Publish — operator can rotate UCANs without
// restarting the publisher — and each distinct delegation lands under
// its own content-addressed <rev>.
func TestRollingV2Publisher_RotatingDelegation(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	calls := 0
	pub.SetDelegationSource(func() ([]byte, error) {
		calls++
		return []byte(fmt.Sprintf("ucan-gen-%d", calls)), nil
	})

	for i := 0; i < 3; i++ {
		_, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x10), 0, nil)
		require.NoError(t, err, "publish %d", i)
	}
	require.Equal(t, 3, calls, "delegation source consulted once per Publish")

	// Three distinct delegations → three Authority UCAN files, each at
	// the <rev> derived from its own content.
	require.Len(t, listAuthorityUCANRevs(t, snapDir), 3)
	for i := 0; i < 3; i++ {
		content := []byte(fmt.Sprintf("ucan-gen-%d", i+1))
		got, err := os.ReadFile(filepath.Join(snapDir, ChainAuthorityUCANFileName(testENRFP, genIDFromContent(content))))
		require.NoError(t, err, "read rotation %d UCAN", i)
		require.Equal(t, content, got, "rotation %d UCAN content", i)
	}
}

// TestRollingV2Publisher_AuthorityUCANStableAcrossGenerations is the
// stability guarantee: republishing with an unchanged delegation reuses
// the exact same Authority UCAN file, torrent, and AuthorityUCANHash —
// so a generation bump does not churn the UCAN advertisement.
func TestRollingV2Publisher_AuthorityUCANStableAcrossGenerations(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	pub.SetDelegationSource(func() ([]byte, error) { return []byte("stable-delegation"), nil })

	// gen 0: the default inventory.
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x10), 0, nil)
	require.NoError(t, err)

	// gen 1: a strict superset (gen 0 stays valid → retained), same
	// delegation.
	inv1 := rollingTestInventory(t, 0x10)
	inv1.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    1024,
		ToStep:      2048,
		Name:        "v1.0-accounts.1024-2048.kv",
		TorrentHash: [20]byte{0x10, 0xcc},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	_, err = pub.Publish(context.Background(), inv1, 0, nil)
	require.NoError(t, err)

	gens := pub.History()
	require.Len(t, gens, 2, "both generations retained")

	// One Authority UCAN file shared by both generations.
	require.Len(t, listAuthorityUCANRevs(t, snapDir), 1,
		"unchanged delegation must not produce a second Authority UCAN file")

	// Both manifests carry the identical AuthorityUCANHash.
	var hashes []string
	for _, g := range gens {
		tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, g)))
		require.NoError(t, err)
		manifest, err := ParseV2(tomlBytes)
		require.NoError(t, err)
		require.NotEmpty(t, manifest.AuthorityUCANHash)
		hashes = append(hashes, manifest.AuthorityUCANHash)
	}
	require.Equal(t, hashes[0], hashes[1],
		"AuthorityUCANHash must be stable across generations with an unchanged delegation")
}

// readAuthorityUCANHash parses the manifest for genID and returns its
// AuthorityUCANHash field.
func readAuthorityUCANHash(t *testing.T, snapDir, genID string) string {
	t.Helper()
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, genID)))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	return manifest.AuthorityUCANHash
}

// TestRollingV2Publisher_AuthorityUCANStableAcrossRestart confirms a
// publisher restart does not churn the Authority UCAN: a fresh publisher
// over the same snapDir, given the same delegation, reuses the existing
// content-addressed sidecar — the <rev>, file, and AuthorityUCANHash all
// survive the restart unchanged.
func TestRollingV2Publisher_AuthorityUCANStableAcrossRestart(t *testing.T) {
	snapDir := t.TempDir()
	delegation := []byte("operator-delegation")

	// Publisher A: publish gen 0 with a delegation source.
	a, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	a.SetENRFingerprint(testENRFP)
	a.SetDelegationSource(func() ([]byte, error) { return delegation, nil })
	_, err = a.Publish(context.Background(), rollingTestInventory(t, 0x60), 0, nil)
	require.NoError(t, err)
	gen0 := a.History()[0]
	beforeHash := readAuthorityUCANHash(t, snapDir, gen0)
	require.NotEmpty(t, beforeHash)

	// Restart: publisher B over the same snapDir, same delegation.
	b, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	b.SetENRFingerprint(testENRFP)
	b.SetDelegationSource(func() ([]byte, error) { return delegation, nil })
	require.Len(t, b.History(), 1, "restart discovers gen 0")

	// Publish gen 1 — a strict superset (gen 0 stays retained), same
	// delegation.
	inv1 := rollingTestInventory(t, 0x60)
	inv1.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    1024,
		ToStep:      2048,
		Name:        "v1.0-accounts.1024-2048.kv",
		TorrentHash: [20]byte{0x60, 0xcc},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	_, err = b.Publish(context.Background(), inv1, 0, nil)
	require.NoError(t, err)

	// One Authority UCAN file, at the rev of the unchanged delegation.
	require.ElementsMatch(t, []string{genIDFromContent(delegation)}, listAuthorityUCANRevs(t, snapDir),
		"restart + republish with an unchanged delegation must not churn the Authority UCAN")

	// Both generations carry the same AuthorityUCANHash as before restart.
	gens := b.History()
	require.Len(t, gens, 2)
	for _, g := range gens {
		require.Equal(t, beforeHash, readAuthorityUCANHash(t, snapDir, g),
			"AuthorityUCANHash must be stable across a restart")
	}
}

// TestRollingV2Publisher_AuthorityUCANSurvivesEviction confirms that
// evicting an invalid generation does NOT delete its Authority UCAN
// sidecar — Authority UCANs are content-addressed and not per-generation.
func TestRollingV2Publisher_AuthorityUCANSurvivesEviction(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	ucan := []byte("u0")
	pub.SetDelegationSource(func() ([]byte, error) { return ucan, nil })

	// gen 0: accounts.0-1024 + storage.0-1024, delegation u0.
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x20), 0, nil)
	require.NoError(t, err)
	gen0Rev := genIDFromContent([]byte("u0"))

	// gen 1: retires both names (gen 0 becomes invalid → evicted) under a
	// rotated delegation u1.
	ucan = []byte("u1")
	inv2 := snapshotinv.NewInventory()
	inv2.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x20, 0xcc},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	_, err = pub.Publish(context.Background(), inv2, 0, nil)
	require.NoError(t, err)
	gen1Rev := genIDFromContent([]byte("u1"))

	require.Len(t, listV2Generations(t, snapDir), 1, "gen 0 evicted")

	// gen 0's Authority UCAN survives its generation's eviction, and
	// gen 1's is present too.
	require.ElementsMatch(t, []string{gen0Rev, gen1Rev}, listAuthorityUCANRevs(t, snapDir),
		"an evicted generation's Authority UCAN must not be deleted")
	for _, rev := range []string{gen0Rev, gen1Rev} {
		_, err = os.Stat(filepath.Join(snapDir, ChainAuthorityUCANFileName(testENRFP, rev)))
		require.NoError(t, err, "Authority UCAN %s must remain on disk", rev)
	}
}

// TestRollingV2Publisher_CleanupLeavesAuthorityUCAN confirms Cleanup()
// does not garbage-collect Authority UCAN sidecars — they are long-lived
// and content-deduped, not per-generation orphans.
func TestRollingV2Publisher_CleanupLeavesAuthorityUCAN(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	pub.SetDelegationSource(func() ([]byte, error) { return []byte("u"), nil })
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x30), 0, nil)
	require.NoError(t, err)

	// A stray Authority UCAN under a valid-shape <rev> that no generation
	// references — Cleanup must leave it (Authority UCANs are not GC'd).
	stray := ChainAuthorityUCANFileName(testENRFP, "ffffffffffffffff")
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, stray), []byte("stray"), 0o644))

	require.NoError(t, pub.Cleanup())

	_, err = os.Stat(filepath.Join(snapDir, stray))
	require.NoError(t, err, "Cleanup must not remove Authority UCAN sidecars")
	require.Len(t, listAuthorityUCANRevs(t, snapDir), 2,
		"both the published and the stray Authority UCAN remain")
}

// TestRollingV2Publisher_EmptyDelegationSkipsPair confirms a delegation
// source that returns (nil, nil) is treated as "no delegation this
// generation" — V2 publishes without a UCAN sidecar.
func TestRollingV2Publisher_EmptyDelegationSkipsPair(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	pub.SetDelegationSource(func() ([]byte, error) { return nil, nil })

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x40), 0, nil)
	require.NoError(t, err)

	require.Len(t, listV2Generations(t, snapDir), 1)
	require.Empty(t, listAuthorityUCANRevs(t, snapDir))

	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, pub.History()[0])))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.Empty(t, manifest.AuthorityUCANHash)
}

// TestRollingV2Publisher_DelegationErrorAbortsPublish confirms a
// delegation-source error propagates out of Publish without writing V2
// (or UCAN) files for that generation.
func TestRollingV2Publisher_DelegationErrorAbortsPublish(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	pub.SetDelegationSource(func() ([]byte, error) {
		return nil, fmt.Errorf("operator key unavailable")
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x50), 0, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "operator key unavailable")

	// No artefacts written for the failed generation.
	require.Empty(t, listV2Generations(t, snapDir))
	require.Empty(t, listAuthorityUCANRevs(t, snapDir))
}
