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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// listUCANGenerations returns sorted seqs of chain.ucan.<seq>.bin files
// in snapDir.
func listUCANGenerations(t *testing.T, snapDir string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	var seqs []uint64
	for _, e := range entries {
		if seq, ok := ParseChainUCANFileName(e.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

// TestRollingV2Publisher_NoDelegationSourceNoUCAN confirms the default
// publisher (no delegation source set) writes V2 only — no UCAN sidecar
// files, and the V2 manifest's UCANHash is empty.
func TestRollingV2Publisher_NoDelegationSourceNoUCAN(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x10), 0, nil)
	require.NoError(t, err)

	require.Equal(t, []uint64{0}, listV2Generations(t, snapDir))
	require.Empty(t, listUCANGenerations(t, snapDir),
		"no delegation source → no UCAN sidecar")

	// The on-disk V2 manifest must carry an empty UCANHash field.
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.Empty(t, manifest.UCANHash)
}

// TestRollingV2Publisher_DelegationSourceWritesPair confirms that with
// a delegation source configured, every Publish() writes the paired
// (V2, UCAN) generation and the V2's UCANHash matches the UCAN
// torrent's infohash.
func TestRollingV2Publisher_DelegationSourceWritesPair(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	// Distinctive bytes so the UCAN torrent has a stable, comparable
	// infohash. Real UCANs are CBOR; we just need bytes-on-disk for
	// the publisher mechanics.
	ucanBytes := []byte("delegation-payload-v1")
	pub.SetDelegationSource(func() ([]byte, error) { return ucanBytes, nil })

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x10), 0, nil)
	require.NoError(t, err)

	require.Equal(t, []uint64{0}, listV2Generations(t, snapDir))
	require.Equal(t, []uint64{0}, listUCANGenerations(t, snapDir))

	// UCAN file content matches what the source returned.
	gotUCAN, err := os.ReadFile(filepath.Join(snapDir, ChainUCANFileNameForSeq(0)))
	require.NoError(t, err)
	require.Equal(t, ucanBytes, gotUCAN)

	// UCAN .torrent exists alongside.
	_, err = os.Stat(filepath.Join(snapDir, ChainUCANFileNameForSeq(0)+".torrent"))
	require.NoError(t, err)

	// V2 manifest's UCANHash matches the UCAN torrent's infohash.
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.NotEmpty(t, manifest.UCANHash)

	tf := NewAtomicTorrentFS(snapDir)
	ucanSpec, err := tf.LoadByName(ChainUCANFileNameForSeq(0) + ".torrent")
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ucanSpec.InfoHash[:]), manifest.UCANHash,
		"V2.UCANHash must equal hex(UCAN torrent infohash)")
}

// TestRollingV2Publisher_RotatingDelegation confirms the source is
// consulted on every Publish — operator can rotate UCANs without
// restarting the publisher.
func TestRollingV2Publisher_RotatingDelegation(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

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

	// Each generation's UCAN bytes match the rotation.
	for i := 0; i < 3; i++ {
		got, err := os.ReadFile(filepath.Join(snapDir, ChainUCANFileNameForSeq(uint64(i))))
		require.NoError(t, err, "read gen %d UCAN", i)
		require.Equal(t, []byte(fmt.Sprintf("ucan-gen-%d", i+1)), got,
			"gen %d UCAN content reflects the rotation", i)
	}
}

// TestRollingV2Publisher_UCANEvictedWithGeneration confirms eviction
// removes both the V2 file and its paired UCAN sidecar (plus their
// .torrent metafiles).
func TestRollingV2Publisher_UCANEvictedWithGeneration(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 2)
	require.NoError(t, err)

	pub.SetDelegationSource(func() ([]byte, error) {
		return []byte("u"), nil
	})

	for i := 0; i < 5; i++ {
		_, err := pub.Publish(context.Background(), rollingTestInventory(t, 0x20), 0, nil)
		require.NoError(t, err)
	}

	// MaxRetained=2 → seqs 3, 4 survive. UCAN sidecars track the same
	// retention window as the V2 manifests they pair with.
	require.Equal(t, []uint64{3, 4}, listV2Generations(t, snapDir))
	require.Equal(t, []uint64{3, 4}, listUCANGenerations(t, snapDir))

	// Evicted .torrent metafiles are gone too.
	for _, evicted := range []uint64{0, 1, 2} {
		ucanName := ChainUCANFileNameForSeq(evicted)
		_, err := os.Stat(filepath.Join(snapDir, ucanName))
		require.True(t, os.IsNotExist(err), "evicted UCAN %s must be gone", ucanName)
		_, err = os.Stat(filepath.Join(snapDir, ucanName+".torrent"))
		require.True(t, os.IsNotExist(err), "evicted UCAN .torrent %s must be gone", ucanName)
	}
}

// TestRollingV2Publisher_CleanupHandlesOrphanUCAN confirms Cleanup()
// also removes orphan chain.ucan.<seq>.bin files (and their torrents)
// for seqs not in current history.
func TestRollingV2Publisher_CleanupHandlesOrphanUCAN(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	pub.SetDelegationSource(func() ([]byte, error) { return []byte("u"), nil })
	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x30), 0, nil)
	require.NoError(t, err)

	// Drop a stray orphan pair from a hypothetical crash.
	require.NoError(t, os.WriteFile(
		filepath.Join(snapDir, ChainUCANFileNameForSeq(99)),
		[]byte("orphan"), 0o644))
	require.NoError(t, os.WriteFile(
		filepath.Join(snapDir, ChainUCANFileNameForSeq(99)+".torrent"),
		[]byte{}, 0o644))

	require.NoError(t, pub.Cleanup())

	require.Equal(t, []uint64{0}, listUCANGenerations(t, snapDir),
		"Cleanup must drop orphan UCAN seqs not in history")
	_, err = os.Stat(filepath.Join(snapDir, ChainUCANFileNameForSeq(99)+".torrent"))
	require.True(t, os.IsNotExist(err), "orphan UCAN .torrent must be cleaned up")
}

// TestRollingV2Publisher_EmptyDelegationSkipsPair confirms a delegation
// source that returns (nil, nil) is treated as "no delegation this
// generation" — V2 publishes without a UCAN sidecar.
func TestRollingV2Publisher_EmptyDelegationSkipsPair(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	pub.SetDelegationSource(func() ([]byte, error) { return nil, nil })

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x40), 0, nil)
	require.NoError(t, err)

	require.Equal(t, []uint64{0}, listV2Generations(t, snapDir))
	require.Empty(t, listUCANGenerations(t, snapDir))

	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileNameForSeq(0)))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.Empty(t, manifest.UCANHash)
}

// TestRollingV2Publisher_DelegationErrorAbortsPublish confirms a
// delegation-source error propagates out of Publish without writing V2
// (or UCAN) files for that generation.
func TestRollingV2Publisher_DelegationErrorAbortsPublish(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	pub.SetDelegationSource(func() ([]byte, error) {
		return nil, fmt.Errorf("operator key unavailable")
	})

	_, err = pub.Publish(context.Background(), rollingTestInventory(t, 0x50), 0, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "operator key unavailable")

	// No artefacts written for the failed generation.
	require.Empty(t, listV2Generations(t, snapDir))
	require.Empty(t, listUCANGenerations(t, snapDir))
}
