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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestRollingV2Publisher_ForkCutFilter_DropsPreCutBlockEntries pins
// end-to-end: a fork-configured publisher inventory contains a mix of
// pre-cut + post-cut block files; the published chain.v2 contains only
// the post-cut entries. The pre-cut bytes stay on disk + BT-seedable
// (the inventory keeps them); they just don't appear in the fork's
// signed manifest.
func TestRollingV2Publisher_ForkCutFilter_DropsPreCutBlockEntries(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)
	pub.SetForkCutBlock(20_000_000, nil)

	inv := snapshotinv.NewInventory()
	// Pre-cut block files (parent lineage — fork-from copied them).
	addBlockFile(t, inv, "v1.0-019998-019999-headers.seg", [20]byte{0xa1})
	addBlockFile(t, inv, "v1.0-019999-020000-headers.seg", [20]byte{0xa2})
	// Post-cut block files (fork's own retire output).
	addBlockFile(t, inv, "v1.0-020001-020002-headers.seg", [20]byte{0xb1})
	addBlockFile(t, inv, "v1.0-020100-020200-headers.seg", [20]byte{0xb2})

	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)

	gens := listV2Generations(t, snapDir)
	require.Len(t, gens, 1)
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, gens[0])))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)

	names := blockNames(manifest.Blocks)
	require.NotContains(t, names, "v1.0-019998-019999-headers.seg",
		"pre-cut block file must not appear in fork manifest")
	require.NotContains(t, names, "v1.0-019999-020000-headers.seg",
		"pre-cut block file (to == cutBlock) must not appear in fork manifest")
	require.Contains(t, names, "v1.0-020001-020002-headers.seg",
		"post-cut block file must appear in fork manifest")
	require.Contains(t, names, "v1.0-020100-020200-headers.seg",
		"post-cut block file must appear in fork manifest")

	// Inventory is preserved — the filter only mutates the manifest
	// emitted, not the inventory itself. Pre-cut bytes remain on
	// disk and would be re-introduced if a future Publish is run
	// against a root-chain publisher (or some other code consults
	// the inventory directly).
	require.Len(t, inv.BlockFiles(), 4,
		"filter is publish-time only — inventory keeps all entries")
}

// TestRollingV2Publisher_ForkCutFilter_DisabledByDefault confirms that
// a root-chain publisher (no SetForkCutBlock) emits every block file
// in the inventory regardless of block numbers — no accidental
// filtering on non-fork chains.
func TestRollingV2Publisher_ForkCutFilter_DisabledByDefault(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)
	// Deliberately NOT calling SetForkCutBlock — root chain default.

	inv := snapshotinv.NewInventory()
	addBlockFile(t, inv, "v1.0-019998-019999-headers.seg", [20]byte{0xa1})
	addBlockFile(t, inv, "v1.0-020001-020002-headers.seg", [20]byte{0xb1})

	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)

	gens := listV2Generations(t, snapDir)
	require.Len(t, gens, 1)
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, gens[0])))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)
	require.Len(t, manifest.Blocks, 2,
		"root-chain publisher emits all inventory block entries — no filter")
}

func addBlockFile(t *testing.T, inv *snapshotinv.Inventory, name string, hash [20]byte) {
	t.Helper()
	inv.AddFile(&snapshotinv.FileEntry{
		Name:        name,
		TorrentHash: hash,
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
}
