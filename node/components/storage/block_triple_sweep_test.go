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

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSweepIncompleteBlockTriples pins the boot-time cleanup of block
// .seg triples that retire aborted mid-build (e.g. transactions
// indexer refused after DumpBlocks hit ErrRangeAheadOfHead, leaving
// headers+bodies without transactions).
func TestSweepIncompleteBlockTriples(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	touch := func(names ...string) {
		for _, n := range names {
			require.NoError(t, os.WriteFile(filepath.Join(dir, n), []byte("stub"), 0o600))
		}
	}

	// A: complete triple — MUST survive.
	touch(
		"v1.1-000000-001000-headers.seg",
		"v1.1-000000-001000-bodies.seg",
		"v1.1-000000-001000-transactions.seg",
	)
	// B: incomplete — headers+bodies present, transactions missing.
	// The exact orphan class the straddler-truncation issue produces.
	// All three members' sidecars (.torrent + .idx + .idx.torrent)
	// must go.
	touch(
		"v1.1-003230-003240-headers.seg",
		"v1.1-003230-003240-headers.seg.torrent",
		"v1.1-003230-003240-headers.idx",
		"v1.1-003230-003240-headers.idx.torrent",
		"v1.1-003230-003240-bodies.seg",
		"v1.1-003230-003240-bodies.seg.torrent",
		"v1.1-003230-003240-bodies.idx",
	)
	// C: solo transactions.seg — MUST be removed too.
	touch(
		"v1.1-003300-003310-transactions.seg",
		"v1.1-003300-003310-transactions.seg.torrent",
	)
	// D: unrelated state-domain files — MUST be untouched (parseBlockSegName rejects them).
	touch(
		"v2.0-accounts.256-288.kv",
		"v2.0-accounts.256-288.kv.torrent",
	)

	removed := sweepIncompleteBlockTriples(dir, nil)

	// A survives.
	require.FileExists(t, filepath.Join(dir, "v1.1-000000-001000-headers.seg"))
	require.FileExists(t, filepath.Join(dir, "v1.1-000000-001000-bodies.seg"))
	require.FileExists(t, filepath.Join(dir, "v1.1-000000-001000-transactions.seg"))

	// B is fully cleaned.
	for _, n := range []string{
		"v1.1-003230-003240-headers.seg",
		"v1.1-003230-003240-headers.seg.torrent",
		"v1.1-003230-003240-headers.idx",
		"v1.1-003230-003240-headers.idx.torrent",
		"v1.1-003230-003240-bodies.seg",
		"v1.1-003230-003240-bodies.seg.torrent",
		"v1.1-003230-003240-bodies.idx",
	} {
		_, err := os.Stat(filepath.Join(dir, n))
		require.True(t, os.IsNotExist(err), "incomplete triple member must be removed: %s", n)
	}

	// C is cleaned.
	_, err := os.Stat(filepath.Join(dir, "v1.1-003300-003310-transactions.seg"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "v1.1-003300-003310-transactions.seg.torrent"))
	require.True(t, os.IsNotExist(err))

	// D untouched.
	require.FileExists(t, filepath.Join(dir, "v2.0-accounts.256-288.kv"))
	require.FileExists(t, filepath.Join(dir, "v2.0-accounts.256-288.kv.torrent"))

	// Returned primaries: 3 (B's headers + B's bodies + C's transactions).
	require.Len(t, removed, 3)
	require.ElementsMatch(t, []string{
		"v1.1-003230-003240-headers.seg",
		"v1.1-003230-003240-bodies.seg",
		"v1.1-003300-003310-transactions.seg",
	}, removed)
}

// TestSweepIncompleteBlockTriples_EmptyDir — a snapDir with no .seg
// files is a safe no-op.
func TestSweepIncompleteBlockTriples_EmptyDir(t *testing.T) {
	t.Parallel()
	require.Nil(t, sweepIncompleteBlockTriples(t.TempDir(), nil))
}

// TestSweepIncompleteBlockTriples_UnreadableDir — a missing snapDir
// returns nil rather than panicking.
func TestSweepIncompleteBlockTriples_UnreadableDir(t *testing.T) {
	t.Parallel()
	require.Nil(t, sweepIncompleteBlockTriples("/nonexistent/path/that/does/not/exist", nil))
	require.Nil(t, sweepIncompleteBlockTriples("", nil))
}
