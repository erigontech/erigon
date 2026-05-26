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

// TestRollingV2Publisher_RetentionFloor_DropsBelowFloor pins end-to-end
// that a minimal-mode publisher with a configured retention floor
// publishes only the domain entries whose range survives the floor,
// and pins each domain's Coverage[0] to the floor.
func TestRollingV2Publisher_RetentionFloor_DropsBelowFloor(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)
	pub.SetRetentionFloor(2048) // minimal-mode publisher's prune-window floor

	inv := snapshotinv.NewInventory()
	// Two pre-floor accounts.kv files — outside the retention window.
	addDomainFile(t, inv, snapshotinv.DomainAccounts, 0, 1024, "v2.0-accounts.0-1024.kv", [20]byte{0xa1})
	addDomainFile(t, inv, snapshotinv.DomainAccounts, 1024, 2048, "v2.0-accounts.1024-2048.kv", [20]byte{0xa2})
	// Two in-window accounts.kv files — kept.
	addDomainFile(t, inv, snapshotinv.DomainAccounts, 2048, 4096, "v2.0-accounts.2048-4096.kv", [20]byte{0xb1})
	addDomainFile(t, inv, snapshotinv.DomainAccounts, 4096, 6144, "v2.0-accounts.4096-6144.kv", [20]byte{0xb2})

	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)

	gens := listV2Generations(t, snapDir)
	require.Len(t, gens, 1)
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, gens[0])))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)

	accounts := manifest.Domains["accounts"]
	require.NotNil(t, accounts)
	require.Len(t, accounts.Files, 2,
		"only in-window files survive the retention-floor filter")
	require.Equal(t, "v2.0-accounts.2048-4096.kv", accounts.Files[0].Name)
	require.Equal(t, "v2.0-accounts.4096-6144.kv", accounts.Files[1].Name)
	require.Equal(t, [2]uint64{2048, 6144}, accounts.Coverage,
		"Coverage[0] is pinned to the floor so consumers know the publisher's served range")

	// Inventory is untouched — the filter is publish-time only.
	require.Len(t, inv.LocalFiles(snapshotinv.DomainAccounts), 4,
		"the filter doesn't mutate the inventory")
}

// TestRollingV2Publisher_RetentionFloor_DisabledByDefault confirms that
// a full-history publisher (no SetRetentionFloor) emits every inventory
// entry — no accidental filtering when the floor is zero.
func TestRollingV2Publisher_RetentionFloor_DisabledByDefault(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)
	// Deliberately NOT calling SetRetentionFloor — full-history default.

	inv := snapshotinv.NewInventory()
	addDomainFile(t, inv, snapshotinv.DomainAccounts, 0, 2048, "v2.0-accounts.0-2048.kv", [20]byte{0xa1})
	addDomainFile(t, inv, snapshotinv.DomainAccounts, 2048, 4096, "v2.0-accounts.2048-4096.kv", [20]byte{0xa2})

	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)

	gens := listV2Generations(t, snapDir)
	require.Len(t, gens, 1)
	tomlBytes, err := os.ReadFile(filepath.Join(snapDir, ChainTomlV2FileName(testENRFP, gens[0])))
	require.NoError(t, err)
	manifest, err := ParseV2(tomlBytes)
	require.NoError(t, err)

	accounts := manifest.Domains["accounts"]
	require.NotNil(t, accounts)
	require.Len(t, accounts.Files, 2, "full-history publisher emits every inventory entry")
	require.Equal(t, [2]uint64{0, 4096}, accounts.Coverage,
		"Coverage[0] tracks the lowest file when no floor is configured")
}

func addDomainFile(t *testing.T, inv *snapshotinv.Inventory, domain snapshotinv.Domain, from, to uint64, name string, hash [20]byte) {
	t.Helper()
	require.NoError(t, inv.AddFile(&snapshotinv.FileEntry{
		Domain:      domain,
		FromStep:    from,
		ToStep:      to,
		Name:        name,
		TorrentHash: hash,
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	}))
}
