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
	"testing"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/stretchr/testify/require"
)

func TestDetectVersion(t *testing.T) {
	// V1: no version field.
	v1 := []byte(`"v1.0-000000-000500-headers.seg" = "abc123"` + "\n")
	require.Equal(t, 1, DetectVersion(v1))

	// V2: has version field.
	v2 := []byte("version = 2\n[blocks]\n")
	require.Equal(t, 2, DetectVersion(v2))

	// Garbage.
	require.Equal(t, 1, DetectVersion([]byte("not valid toml {{{")))
}

func TestGenerateV2FromInventory(t *testing.T) {
	inv := snapshotinv.NewInventory()

	// Add block files.
	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.0-000000-000500-headers.seg",
		TorrentHash: [20]byte{0xab, 0xcd},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})

	// Add domain files — mix of canonical and non-canonical.
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
		ToStep:      3072,
		Name:        "v1.0-accounts.2048-3072.kv",
		TorrentHash: [20]byte{0x33, 0x44},
		Local:       true,
		Trust:       snapshotinv.TrustConsensus,
	})
	// Non-canonical file (size 100, not power-of-2) — should be excluded.
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    3072,
		ToStep:      3172,
		Name:        "v1.0-accounts.3072-3172.kv",
		TorrentHash: [20]byte{0x55, 0x66},
		Local:       true,
		Trust:       snapshotinv.TrustNone,
	})

	manifest := GenerateV2(inv)

	require.Equal(t, ChainTomlV2Version, manifest.Version)
	require.Len(t, manifest.Blocks, 1)
	require.Contains(t, manifest.Blocks, "v1.0-000000-000500-headers.seg")

	// Domain: only canonical files included.
	acc := manifest.Domains["accounts"]
	require.NotNil(t, acc)
	require.Len(t, acc.Files, 2) // 0-2048 and 2048-3072 (both canonical), 3072-3172 excluded (non-pow2)
	require.Equal(t, "v1.0-accounts.0-2048.kv", acc.Files[0].Name)
	require.Equal(t, "verified", acc.Files[0].Trust)
	require.Equal(t, "v1.0-accounts.2048-3072.kv", acc.Files[1].Name)
	require.Equal(t, "consensus", acc.Files[1].Trust)
}

func TestV2MarshalRoundTrip(t *testing.T) {
	inv := snapshotinv.NewInventory()
	inv.AddFile(&snapshotinv.FileEntry{
		Name:        "v1.0-000000-000500-headers.seg",
		TorrentHash: [20]byte{0xab},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      4096,
		Name:        "v1.0-accounts.0-4096.kv",
		TorrentHash: [20]byte{0xcd},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})

	original := GenerateV2(inv)

	data, err := MarshalV2(original)
	require.NoError(t, err)

	parsed, err := ParseV2(data)
	require.NoError(t, err)

	require.Equal(t, original.Version, parsed.Version)
	require.Equal(t, original.Blocks, parsed.Blocks)
	require.Len(t, parsed.Domains, 1)
	require.Len(t, parsed.Domains["accounts"].Files, 1)
	require.Equal(t, original.Domains["accounts"].Files[0].Name, parsed.Domains["accounts"].Files[0].Name)
}

func TestParseV2RejectsV1(t *testing.T) {
	v1 := []byte(`"v1.0-headers.seg" = "abc123"` + "\n")
	_, err := ParseV2(v1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}

func TestDomainManifestStepRanges(t *testing.T) {
	dm := &DomainManifest{
		Coverage: [2]uint64{0, 4096},
		Files: []DomainFileEntry{
			{Name: "a.kv", Range: [2]uint64{0, 2048}, Hash: "aa", Trust: "verified"},
			{Name: "b.kv", Range: [2]uint64{2048, 4096}, Hash: "bb", Trust: "consensus"},
		},
	}

	ranges := dm.StepRanges()
	require.True(t, ranges.IsComplete(0, 4096))
	require.Equal(t, uint64(4096), ranges.Coverage())
}

func TestDomainManifestFilesAtTrust(t *testing.T) {
	dm := &DomainManifest{
		Files: []DomainFileEntry{
			{Name: "a.kv", Trust: "verified"},
			{Name: "b.kv", Trust: "consensus"},
			{Name: "c.kv", Trust: "none"},
		},
	}

	require.Len(t, dm.FilesAtTrust(snapshotinv.TrustNone), 3)
	require.Len(t, dm.FilesAtTrust(snapshotinv.TrustConsensus), 2)
	require.Len(t, dm.FilesAtTrust(snapshotinv.TrustVerified), 1)
}

func TestIsCanonicalFile(t *testing.T) {
	// Canonical: power-of-2, aligned.
	require.True(t, isCanonicalFile(snapshotinv.StepRange{From: 0, To: 2048}))
	require.True(t, isCanonicalFile(snapshotinv.StepRange{From: 2048, To: 4096}))
	require.True(t, isCanonicalFile(snapshotinv.StepRange{From: 4096, To: 5120}))

	// Non-canonical: not power-of-2.
	require.False(t, isCanonicalFile(snapshotinv.StepRange{From: 0, To: 3000}))
	require.False(t, isCanonicalFile(snapshotinv.StepRange{From: 0, To: 100}))

	// Non-canonical: misaligned.
	require.False(t, isCanonicalFile(snapshotinv.StepRange{From: 100, To: 1124}))
}
