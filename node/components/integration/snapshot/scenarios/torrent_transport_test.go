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

package scenarios_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestTorrent_TwoNodeGapFill drives the real-network Transport: two in-
// process nodes with anacrolix/torrent clients, a seeder with real file
// bytes on disk, a leecher that discovers the seeder's address via a
// direct AddPeer call (no DHT, no tracker), and a PeerManifestReceived
// that walks through the same orchestrator path as the simulated scenario.
//
// The assertion is end-to-end: the leecher's local file matches the bytes
// the seeder wrote.
//
// Skipped under -short: real torrent transfer takes a few seconds even on
// tiny data sets.
func TestTorrent_TwoNodeGapFill(t *testing.T) {
	if testing.Short() {
		t.Skip("torrent-transport tests skipped under -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Seeder -------------------------------------------------------
	seederDir := t.TempDir()
	seeder := harness.NewNode(nil)
	t.Cleanup(func() { _ = seeder.Close() })

	seederTransport, err := harness.NewTorrentTransport(seeder.Bus, seederDir)
	require.NoError(t, err)
	seeder.Transport = seederTransport
	require.NoError(t, seeder.Start(ctx))

	// Write a real file and register it with the seeder's torrent client.
	accountsName := "v1.0-accounts.0-256.kv"
	accountsBytes := bytesOfLength(4096, 'A')
	accountsHash, err := seederTransport.Seed(accountsName, accountsBytes)
	require.NoError(t, err)

	// Reflect the seeded file in the seeder's inventory so the manifest
	// announcement carries the correct torrent hash + range.
	seeder.Inventory.AddFile(&snapshot.FileEntry{
		Domain:      snapshot.DomainAccounts,
		FromStep:    0,
		ToStep:      256,
		Name:        accountsName,
		Size:        int64(len(accountsBytes)),
		TorrentHash: accountsHash,
		Local:       true,
		Trust:       snapshot.TrustVerified,
	})

	// --- Leecher ------------------------------------------------------
	leecherDir := t.TempDir()
	leecher := harness.NewNode(nil)
	t.Cleanup(func() { _ = leecher.Close() })

	leecherTransport, err := harness.NewTorrentTransport(leecher.Bus, leecherDir)
	require.NoError(t, err)
	leecher.Transport = leecherTransport
	require.NoError(t, leecher.Start(ctx))

	// Inject the seeder's address — this is the "discovery" step that the
	// production path would do via ENR BT port lookup.
	leecherTransport.AddPeer("seeder", "127.0.0.1", seederTransport.LocalPort())

	// --- Announce + wait for transfer --------------------------------
	leecher.Bus.Publish(flow.PeerManifestReceived{
		PeerID: "seeder",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainAccounts: seeder.Inventory.AllDomainFiles(snapshot.DomainAccounts),
		},
	})

	// DownloadComplete arrives once the real torrent transfer finishes.
	require.Eventually(t, func() bool {
		files := leecher.Inventory.LocalFiles(snapshot.DomainAccounts)
		return len(files) == 1 && files[0].Name == accountsName
	}, 30*time.Second, 50*time.Millisecond, "leecher should receive the accounts file")

	// File bytes on disk match the seeder's.
	got, err := os.ReadFile(filepath.Join(leecherDir, accountsName))
	require.NoError(t, err)
	require.Equal(t, accountsBytes, got, "downloaded bytes must match seeded bytes")

	// No failures, no orphan pending, trust was promoted.
	require.Equal(t, 0, leecher.Bus.CountOfType(reflect.TypeOf(flow.DownloadFailed{})),
		"no DownloadFailed should fire in the happy path")
	require.Equal(t, 0, leecher.Orch.PendingCount(),
		"pending should drain after transfer completes")
	require.GreaterOrEqual(t, leecher.Bus.CountOfType(reflect.TypeOf(flow.TrustPromoted{})), 1,
		"TrustPromoted should fire for the received file")
}

func bytesOfLength(n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}
