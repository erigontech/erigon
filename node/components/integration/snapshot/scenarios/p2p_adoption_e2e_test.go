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

// Phase 7 — end-to-end staged canonical adoption. A minority publisher
// holds a file at a non-canonical hash; RunStagedAdoption, driven through
// the REAL storage.Provider, carries the minority verdict all the way:
// fetch the canonical bytes from a swarm peer → Stage 1 + Stage 2
// validation against the staged files → atomic cutover over the live
// file. The unit tests cover the decision paths and cutoverStagedBatch in
// isolation; these exercise the whole pipeline wired together.

package scenarios_test

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	storagecomp "github.com/erigontech/erigon/node/components/storage"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// adoptionFixtureName is a state-domain primary — accounts files are
// neither commitment nor receipt, so Stage 2 builds the overlay but no
// cross-file validator opens the staged file. That keeps the e2e focused
// on the fetch → stage → cutover wiring (the deep commitment/receipt/
// header validators have their own unit coverage).
const adoptionFixtureName = "v1.0-accounts.0-2048.kv"

// adoptionLiveRelPath is where a domain .kv lives on disk — PathForName
// resolves the bare name into the domain/ subdir, so the cutover swaps a
// file at this path.
const adoptionLiveRelPath = "domain/" + adoptionFixtureName

// adoptionSetup builds a canonical seeder + a real-Provider minority node
// that holds adoptionFixtureName at a non-canonical hash (recorded Local
// in its inventory), peered so a fetch can reach the canonical bytes. It
// returns the minority node, the canonical + minority content, and the
// minority verdict pointing the file at the canonical hash.
func adoptionSetup(t *testing.T, logger log.Logger) (minority *harness.P2PNode, canonicalContent, minorityContent []byte, verdict *snapshotsync.MinorityVerdict) {
	t.Helper()
	canonicalNode := harness.NewP2PNode(t, logger)
	minority = harness.NewP2PNodeWithStorageProvider(t, logger)

	// Canonical seeder offers the agreed bytes under the bare name, so
	// FetchCanonicalBatch's torrent-name guard matches the verdict entry.
	canonicalContent = multiPieceFixtureBytes("canonical-adopt", 2<<20)
	canonicalHash := canonicalNode.SeedFile(adoptionFixtureName, canonicalContent, snapshot.DomainAccounts, 0, 2048)

	// The minority node holds a different-hash version of the same file
	// in its kind subdir, recorded Local — its losing-algorithm retire
	// output, the file the cutover must replace.
	minorityContent = multiPieceFixtureBytes("minority-adopt", 2<<20)
	minorityHash := seedSubdirFile(t, minority, adoptionLiveRelPath, minorityContent,
		&snapshot.FileEntry{Name: adoptionFixtureName, Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 0, ToStep: 2048})
	require.NotEqual(t, canonicalHash, minorityHash, "fixture hashes must differ")

	minority.AddSeederPeer(canonicalNode)

	verdict = &snapshotsync.MinorityVerdict{
		Adopt: []snapshotsync.AdvertisementMismatch{{
			Name:          adoptionFixtureName,
			OwnHash:       hex.EncodeToString(minorityHash[:]),
			CanonicalHash: hex.EncodeToString(canonicalHash[:]),
		}},
	}
	return minority, canonicalContent, minorityContent, verdict
}

// TestP2P_StagedAdoption_AutoCutover drives the full policy=auto pipeline
// through the real storage.Provider: a minority verdict → fetch the
// canonical file from a peer → Stage 1 + Stage 2 → atomic cutover. After
// it, the live file holds the canonical bytes and the inventory hash is
// re-stamped to canonical.
func TestP2P_StagedAdoption_AutoCutover(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	minority, canonicalContent, _, verdict := adoptionSetup(t, logger)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	result, err := minority.StorageProvider().RunStagedAdoption(ctx, storagecomp.AdoptionRequest{
		Verdict:          verdict,
		Policy:           snapshotsync.AdoptionAuto,
		CanonicalVersion: "1",
		PruneMode:        prune.DefaultMode,
		Downloader:       minority.Downloader,
	})
	require.NoError(t, err)
	require.Equal(t, storagecomp.AdoptionCutOver, result.Outcome, result.Reason)

	// The live file now holds the canonical bytes.
	liveContent, err := os.ReadFile(filepath.Join(minority.Dirs.Snap, adoptionLiveRelPath))
	require.NoError(t, err)
	require.Equal(t, canonicalContent, liveContent, "live file must be the canonical content after cutover")

	// Inventory entry re-stamped to the canonical hash.
	entry, ok := minority.Inventory.GetByName(adoptionFixtureName)
	require.True(t, ok, "fixture must still be in the inventory")
	require.Equal(t, verdict.Adopt[0].CanonicalHash, hex.EncodeToString(entry.TorrentHash[:]),
		"inventory hash must be re-stamped to canonical after cutover")

	// Staging directory removed.
	_, statErr := os.Stat(filepath.Join(minority.Dirs.Tmp, "adoption-1"))
	require.True(t, os.IsNotExist(statErr), "staging dir must be removed after cutover")
}

// TestP2P_StagedAdoption_StagePolicy drives policy=stage: the canonical
// file is fetched and validated, the ready marker is written, but the
// live file is left untouched for an operator-triggered cutover.
func TestP2P_StagedAdoption_StagePolicy(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	minority, _, minorityContent, verdict := adoptionSetup(t, logger)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	result, err := minority.StorageProvider().RunStagedAdoption(ctx, storagecomp.AdoptionRequest{
		Verdict:          verdict,
		Policy:           snapshotsync.AdoptionStage,
		CanonicalVersion: "2",
		PruneMode:        prune.DefaultMode,
		Downloader:       minority.Downloader,
	})
	require.NoError(t, err)
	require.Equal(t, storagecomp.AdoptionStaged, result.Outcome, result.Reason)
	require.NotNil(t, result.Batch)

	// Validated batch waits on disk with the intent marker.
	stagingDir := filepath.Join(minority.Dirs.Tmp, "adoption-2")
	_, err = os.Stat(filepath.Join(stagingDir, snapshot.AdoptionReadyMarker))
	require.NoError(t, err, "ready marker must be written once the staged batch validates")

	// The live (minority) file is NOT swapped — policy=stage stops short
	// of cutover.
	liveContent, err := os.ReadFile(filepath.Join(minority.Dirs.Snap, adoptionLiveRelPath))
	require.NoError(t, err)
	require.Equal(t, minorityContent, liveContent, "policy=stage must leave the live file untouched")
}

// TestP2P_StagedAdoption_SaltDivergenceAborts pins the salt guard: a
// verdict that includes a salt file aborts before any fetch — a salt
// change invalidates every accessor, a wholesale reindex the operator
// must handle, not an incremental adoption.
func TestP2P_StagedAdoption_SaltDivergenceAborts(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	minority := harness.NewP2PNodeWithStorageProvider(t, logger)

	ownHash := [20]byte{0x01, 0x02, 0x03}
	canonicalHash := [20]byte{0x09, 0x08, 0x07}
	verdict := &snapshotsync.MinorityVerdict{
		Adopt: []snapshotsync.AdvertisementMismatch{{
			Name:          "salt-state.txt",
			OwnHash:       hex.EncodeToString(ownHash[:]),
			CanonicalHash: hex.EncodeToString(canonicalHash[:]),
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	result, err := minority.StorageProvider().RunStagedAdoption(ctx, storagecomp.AdoptionRequest{
		Verdict:          verdict,
		Policy:           snapshotsync.AdoptionAuto,
		CanonicalVersion: "3",
		PruneMode:        prune.DefaultMode,
		Downloader:       minority.Downloader,
	})
	require.NoError(t, err)
	require.Equal(t, storagecomp.AdoptionSaltDivergence, result.Outcome, result.Reason)

	// Aborted before any fetch — no staging directory created.
	_, statErr := os.Stat(filepath.Join(minority.Dirs.Tmp, "adoption-3"))
	require.True(t, os.IsNotExist(statErr), "salt divergence must abort before staging anything")
}
