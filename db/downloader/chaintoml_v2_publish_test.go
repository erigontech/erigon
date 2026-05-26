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
	domainSteps, mergeDepth, minStep := ComputeENRFields(manifest)

	// accounts has two canonical files [0,2048) + [2048,4096); Coverage[1] = 4096.
	require.Equal(t, uint64(4096), domainSteps, "DomainSteps = max Coverage[1] across domains")
	// Largest file is 2048 steps wide.
	require.Equal(t, uint64(2048), mergeDepth, "MergeDepth = largest canonical file size")
	// Floor is the smallest Coverage[0] across domains; full-history publisher has 0.
	require.Zero(t, minStep, "MinStep = floor of advertised step range; 0 for a full-history publisher")
}

func TestComputeENRFieldsEmptyManifest(t *testing.T) {
	empty := &ChainTomlV2{Version: ChainTomlV2Version}
	domainSteps, mergeDepth, minStep := ComputeENRFields(empty)
	require.Zero(t, domainSteps)
	require.Zero(t, mergeDepth)
	require.Zero(t, minStep)

	var nilManifest *ChainTomlV2
	domainSteps, mergeDepth, minStep = ComputeENRFields(nilManifest)
	require.Zero(t, domainSteps)
	require.Zero(t, mergeDepth)
	require.Zero(t, minStep)
}

// TestComputeENRFields_MinStepFromFloor confirms MinStep reflects a
// pruned publisher's retention floor: with the smallest Coverage[0] at
// step 1000, the ENR advertises MinStep=1000 so consumers wanting
// older history skip this peer.
func TestComputeENRFields_MinStepFromFloor(t *testing.T) {
	pruned := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Domains: map[string]*DomainManifest{
			"accounts": {Coverage: [2]uint64{1000, 4096}},
			"storage":  {Coverage: [2]uint64{1200, 4096}},
		},
	}
	_, _, minStep := ComputeENRFields(pruned)
	require.Equal(t, uint64(1000), minStep,
		"MinStep is the smallest Coverage[0] — the lowest floor a consumer can request from this peer")
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

	hash, err := PublishChainTomlV2(snapDir, torrentFS, inv, 12345, testENRFP, enrUpdater)
	require.NoError(t, err)
	require.NotEqual(t, [20]byte{}, hash, "infohash must be non-zero after publish")

	// The file was written — exactly one chain.v2.*.toml generation.
	genIDs := listV2Generations(t, snapDir)
	require.Len(t, genIDs, 1)
	gen0 := ChainTomlV2FileName(testENRFP, genIDs[0])
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, gen0))
	require.NoError(t, err)
	require.NotEmpty(t, tomlBytes)
	require.Equal(t, 2, DetectVersion(tomlBytes), "on-disk file must parse as V2")

	// The torrent file was built.
	torrentExists, err := torrentFS.Exists(gen0)
	require.NoError(t, err)
	require.True(t, torrentExists, "%s.torrent must exist", gen0)

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

	_, err := PublishChainTomlV2(snapDir, torrentFS, inv, 0, testENRFP, nil)
	require.NoError(t, err, "nil enrUpdater must be a tolerated no-op")

	// A chain.v2.*.toml was still written even with a nil ENR updater.
	require.Len(t, listV2Generations(t, snapDir), 1,
		"a chain.v2 manifest must be written even when ENR updater is nil")
}

func TestPublishChainTomlV2Regenerates(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)
	inv := seedInventory(t)

	hash1, err := PublishChainTomlV2(snapDir, torrentFS, inv, 0, testENRFP, nil)
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

	hash2, err := PublishChainTomlV2(snapDir, torrentFS, inv, 0, testENRFP, nil)
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2, "infohash must change when inventory changes")
}

func TestPublishChainTomlV2RejectsNil(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)

	_, err := PublishChainTomlV2(snapDir, torrentFS, nil, 0, testENRFP, nil)
	require.Error(t, err)

	_, err = PublishChainTomlV2(snapDir, nil, seedInventory(t), 0, testENRFP, nil)
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
