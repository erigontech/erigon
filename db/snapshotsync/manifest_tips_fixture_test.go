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

package snapshotsync

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/preverified"
)

// TestDeriveManifestTips_MainnetFixture_2026_05_15 pins what
// DeriveManifestTips computes for a captured snapshot of mainnet's
// upstream preverified.toml from 2026-05-15.
//
// This is the canonical-side determinism test. Given a fixed input
// (the captured fixture), the helper returns specific values. Any
// change to the helper that perturbs these values trips this test;
// any change to the fixture content (whether deliberate refresh or
// accidental corruption) trips the SHA256 sentinel.
//
// Per the three-layer model
// (docs/plans/20260515-three-layer-snapshot-distribution.md), tests
// MUST consume fixture inputs not live registry fetches — only
// fixtures give the determinism this contract is built on.
//
// To refresh the fixture intentionally (e.g., after an upstream
// registry update we want to track):
//   1. Capture a newer chain.toml from /erigon/tmp/.../preverified.toml
//      or wherever the live upstream is reachable.
//   2. Update the SHA256 sentinel below to the new file's hash.
//   3. Update the pinned (BlockTip, StateTipStep) values to match
//      what DeriveManifestTips returns for the new content.
//   4. Commit both the new fixture file AND the test changes in the
//      same commit so reviewers can verify the derivation.
func TestDeriveManifestTips_MainnetFixture_2026_05_15(t *testing.T) {
	t.Parallel()

	fixturePath := filepath.Join("testdata", "snapshot-flow",
		"mainnet-2026-05-15-upstream.toml")

	data, err := os.ReadFile(fixturePath)
	require.NoError(t, err, "fixture must exist at %s", fixturePath)

	// Sentinel: any change to the fixture content trips this. Catches
	// accidental edits, file truncation, and copy-paste errors that
	// would otherwise let the test pass against unintended content.
	actualHash := sha256.Sum256(data)
	const expectedHash = "04476b55fb7a6e2dd435894d0760614f5c680ce7d855c5a1fafef0aab46a52c0"
	require.Equal(t, expectedHash, hex.EncodeToString(actualHash[:]),
		"fixture file SHA256 changed — either an unintentional edit, or refresh the sentinel + pinned tips together")

	// Parse via the preverified package's public API. Same code path
	// LoadSnapshotsHashes uses at runtime, so the test pins what the
	// production loader will compute.
	var rawMap map[string]string
	require.NoError(t, toml.Unmarshal(data, &rawMap),
		"fixture must parse as flat TOML key=value")

	items := preverified.ItemsFromMap(rawMap)
	require.NotEmpty(t, items, "fixture must contain entries")

	// Pinned derivation. These values are what DeriveManifestTips
	// returns for THIS specific fixture content; changes here mean
	// either (a) the helper's algorithm changed, or (b) the fixture
	// was refreshed and the pins need updating to match.
	tips := DeriveManifestTips(items)

	const expectedBlockTip uint64 = 25_069_999     // highest block where headers+bodies+transactions all advertised
	const expectedStateTip uint64 = 8984           // commitment.kv top step
	require.Equal(t, expectedBlockTip, tips.BlockTip,
		"BlockTip mismatch — helper algorithm changed, or fixture refreshed without updating pin")
	require.Equal(t, expectedStateTip, tips.StateTipStep,
		"StateTipStep mismatch — helper algorithm changed, or fixture refreshed without updating pin")

	// Cross-check: count of entries by category. These aren't strictly
	// required for the tips themselves, but they pin the shape of the
	// fixture for downstream tests (validateAdvertisement,
	// HeldRanges, etc.) that will be built against the same fixture.
	const expectedTotalEntries = 7346
	require.Equal(t, expectedTotalEntries, len(items),
		"fixture entry count changed — refresh requires updating count pin")
}
