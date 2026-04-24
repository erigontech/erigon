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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// seedInventory populates an inventory with a canonical block file and two
// canonical accounts files, enough to exercise GenerateV2 + ENR field
// computation without reaching into non-canonical territory.
func seedInventory(t *testing.T) *snapshotinv.Inventory {
	t.Helper()
	inv := snapshotinv.NewInventory()

	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.0-000000-000500-headers.seg",
		TorrentHash: [20]byte{0xaa, 0xbb},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})

	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x11, 0x22},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    2048,
		ToStep:      4096,
		Name:        "v1.0-accounts.2048-4096.kv",
		TorrentHash: [20]byte{0x33, 0x44},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	return inv
}

func TestComputeENRFieldsFromManifest(t *testing.T) {
	inv := seedInventory(t)
	manifest := GenerateV2(inv)
	domainSteps, mergeDepth := ComputeENRFields(manifest)

	// accounts has two canonical files [0,2048) + [2048,4096); Coverage[1] = 4096.
	require.Equal(t, uint64(4096), domainSteps, "DomainSteps = max Coverage[1] across domains")
	// Largest file is 2048 steps wide.
	require.Equal(t, uint64(2048), mergeDepth, "MergeDepth = largest canonical file size")
}

func TestComputeENRFieldsEmptyManifest(t *testing.T) {
	empty := &ChainTomlV2{Version: ChainTomlV2Version}
	domainSteps, mergeDepth := ComputeENRFields(empty)
	require.Zero(t, domainSteps)
	require.Zero(t, mergeDepth)

	var nilManifest *ChainTomlV2
	domainSteps, mergeDepth = ComputeENRFields(nilManifest)
	require.Zero(t, domainSteps)
	require.Zero(t, mergeDepth)
}

func TestPublishChainTomlV2Roundtrip(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)
	inv := seedInventory(t)

	var capturedENR enr.ChainToml
	var enrCalls int
	enrUpdater := func(ct enr.ChainToml) {
		capturedENR = ct
		enrCalls++
	}

	hash, err := PublishChainTomlV2(snapDir, torrentFS, inv, 12345, enrUpdater)
	require.NoError(t, err)
	require.NotEqual(t, [20]byte{}, hash, "infohash must be non-zero after publish")

	// The file was written.
	tomlBytes, err := os.ReadFile(ChainTomlV2Path(snapDir))
	require.NoError(t, err)
	require.NotEmpty(t, tomlBytes)
	require.Equal(t, 2, DetectVersion(tomlBytes), "on-disk file must parse as V2")

	// The torrent file was built.
	torrentExists, err := torrentFS.Exists(ChainTomlV2FileName)
	require.NoError(t, err)
	require.True(t, torrentExists, "chain.toml.v2.torrent must exist")

	// ENR updater was called exactly once with the computed fields.
	require.Equal(t, 1, enrCalls)
	require.Equal(t, uint64(12345), capturedENR.AuthoritativeBlocks)
	require.Equal(t, uint64(12345), capturedENR.KnownBlocks)
	require.Equal(t, [20]byte(hash), capturedENR.InfoHash,
		"ENR InfoHash must match the V2 torrent hash")
	require.Equal(t, uint64(4096), capturedENR.DomainSteps)
	require.Equal(t, uint64(2048), capturedENR.MergeDepth)
}

func TestPublishChainTomlV2NilEnrUpdaterSkipped(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)
	inv := seedInventory(t)

	_, err := PublishChainTomlV2(snapDir, torrentFS, inv, 0, nil)
	require.NoError(t, err, "nil enrUpdater must be a tolerated no-op")

	// File was still written.
	exists, err := dirExists(filepath.Join(snapDir, ChainTomlV2FileName))
	require.NoError(t, err)
	require.True(t, exists, "chain.toml.v2 must be written even when ENR updater is nil")
}

func TestPublishChainTomlV2Regenerates(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)
	inv := seedInventory(t)

	hash1, err := PublishChainTomlV2(snapDir, torrentFS, inv, 0, nil)
	require.NoError(t, err)

	// Add another canonical file; the manifest and its hash must change.
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainStorage,
		FromStep:    0,
		ToStep:      1024,
		Name:        "v1.0-storage.0-1024.kv",
		TorrentHash: [20]byte{0x77, 0x88},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})

	hash2, err := PublishChainTomlV2(snapDir, torrentFS, inv, 0, nil)
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2, "infohash must change when inventory changes")
}

func TestPublishChainTomlV2RejectsNil(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)

	_, err := PublishChainTomlV2(snapDir, torrentFS, nil, 0, nil)
	require.Error(t, err)

	_, err = PublishChainTomlV2(snapDir, nil, seedInventory(t), 0, nil)
	require.Error(t, err)
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
