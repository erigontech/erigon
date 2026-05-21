//go:build p2p_integration

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

// Phase 7b-2 — scoped staging fetch. A minority publisher holds a file
// at a non-canonical hash; FetchCanonicalBatch fetches the canonical
// bytes into an isolated <snap>/.staging-<gen>/ directory using the
// live torrent client, leaving the live (minority) file untouched.

package scenarios_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestP2P_StagedAdoption_FetchCanonicalBatch(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	canonicalNode := harness.NewP2PNode(t, logger)
	minorityNode := harness.NewP2PNode(t, logger)

	fixtureName := "v1.0-accounts.0-2048.kv"

	// The canonical node seeds the agreed-upon bytes.
	canonicalContent := multiPieceFixtureBytes("canonical", 2<<20)
	canonicalHash := canonicalNode.SeedFile(fixtureName, canonicalContent, snapshot.DomainAccounts, 0, 2048)

	// The minority node holds a DIFFERENT version of the same file on
	// disk — its live snapshot, retired by a losing algorithm. Written
	// directly (not seeded) so it stands in for an untouched live file.
	minorityContent := multiPieceFixtureBytes("minority", 2<<20)
	livePath := filepath.Join(minorityNode.Dirs.Snap, fixtureName)
	require.NoError(t, os.WriteFile(livePath, minorityContent, 0o644))

	minorityNode.AddSeederPeer(canonicalNode)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	batch, err := minorityNode.Downloader.FetchCanonicalBatch(ctx, "7", []downloader.CanonicalFile{
		{Name: fixtureName, InfoHash: canonicalHash},
	})
	require.NoError(t, err)
	require.NotNil(t, batch)

	// Staged into the per-generation isolated directory.
	require.Equal(t, filepath.Join(minorityNode.Dirs.Snap, ".staging-7"), batch.Dir)
	require.Len(t, batch.Files, 1)
	require.Equal(t, fixtureName, batch.Files[0].Name)
	require.Equal(t, canonicalHash, batch.Files[0].InfoHash)

	// The staged file holds the canonical bytes.
	stagedContent, err := os.ReadFile(batch.Files[0].Path)
	require.NoError(t, err)
	require.Equal(t, canonicalContent, stagedContent, "staged file must be the canonical content")

	// The live (minority) file is untouched — 7b stages only.
	liveContent, err := os.ReadFile(livePath)
	require.NoError(t, err)
	require.Equal(t, minorityContent, liveContent, "live file must not be modified by staging")
}

// TestP2P_StagedAdoption_NameMismatchAbortsBatch pins the safety check:
// a canonical info-hash whose torrent carries a name other than the one
// the verdict expects aborts the whole batch and removes the staging
// directory. The later cutover renames staged → live by name, so a
// mismatch must never produce a staged file.
func TestP2P_StagedAdoption_NameMismatchAbortsBatch(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	canonicalNode := harness.NewP2PNode(t, logger)
	minorityNode := harness.NewP2PNode(t, logger)

	seededName := "v1.0-accounts.0-2048.kv"
	content := multiPieceFixtureBytes("mismatch", 2<<20)
	hash := canonicalNode.SeedFile(seededName, content, snapshot.DomainAccounts, 0, 2048)

	minorityNode.AddSeederPeer(canonicalNode)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	// Ask for the hash under a name that disagrees with the torrent's.
	_, err := minorityNode.Downloader.FetchCanonicalBatch(ctx, "9", []downloader.CanonicalFile{
		{Name: "v1.0-storage.0-2048.kv", InfoHash: hash},
	})
	require.ErrorContains(t, err, "does not match expected")

	_, statErr := os.Stat(filepath.Join(minorityNode.Dirs.Snap, ".staging-9"))
	require.True(t, os.IsNotExist(statErr), "failed batch must remove the staging directory")
}
