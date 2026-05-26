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

package state_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// TestUpgrade_RefV20Sources_ReadsCorrectly_in_V21World simulates the 3.4 → 3.6 upgrade scenario:
// the on-disk datadir contains v2.0 commitment .kv files whose merged ranges carry referenced
// (shortened) keys, but the running code has AggregatorSqueezeCommitmentValues=false and the
// schema's current commitment kv version is v2.1. The read path must still deref source refs —
// it can no longer gate on the writer-side config flag, because that flag is off but the source
// files still contain refs.
func TestUpgrade_RefV20Sources_ReadsCorrectly_in_V21World(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Phase 1: 3.4-era setup — force schema commitment kv to v2.0 so freshly written files
	// land at v2.0.
	prevVer := statecfg.Schema.CommitmentDomain.FileVersion.DataKV
	statecfg.Schema.CommitmentDomain.FileVersion.DataKV = version.Versions{
		Current:      version.V2_0,
		MinSupported: version.V1_0,
	}
	t.Cleanup(func() {
		statecfg.Schema.CommitmentDomain.FileVersion.DataKV = prevVer
	})

	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: false}
	db, agg := testDbAggregatorWithFiles(t, cfg)
	dirs := agg.Dirs()

	// Squeeze the merged commitment files so they actually carry refs (the build pipeline's
	// own merge path produces full-key output regardless of config; squeeze is now the only
	// producer of referenced keys, mirroring how older datadirs reached this state).
	// Squeeze removes the old .kvi/.bt accessors when it rewrites the .kv file, so we have
	// to reopen the folder and rebuild the missed accessors before any read path is exercised.
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)
		require.NoError(t, state.SqueezeCommitmentFiles(t.Context(), state.AggTx(rwTx), log.New()))
		require.NoError(t, rwTx.Commit())
		require.NoError(t, agg.OpenFolder())
		require.NoError(t, agg.BuildMissedAccessors(t.Context(), 4))
		require.NoError(t, agg.OpenFolder())
	}

	// Confirm at least one v2.0 commitment kv file is now on disk — sanity check that the
	// schema override took effect.
	files, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	require.NoError(t, err)
	var v20Files []string
	for _, f := range files {
		if strings.Contains(f, "commitment") && strings.Contains(f, "v2.0-") {
			v20Files = append(v20Files, f)
		}
	}
	require.NotEmpty(t, v20Files, "expected v2.0 commitment kv files on disk after squeeze")

	// Capture the pre-upgrade root from the v2.0 ref files.
	preRoot := readStateRoot(t, db)
	require.NotEmpty(t, preRoot)

	// Phase 2: switch to the 3.6 world — schema's current kv version is v2.1 and the
	// ReplaceKeysInValues flag is off. Existing files keep their v2.0 dataVer (parsed from
	// the filename at open time), but new merges would emit v2.1 noref.
	statecfg.Schema.CommitmentDomain.FileVersion.DataKV = version.Versions{
		Current:      version.V2_1,
		MinSupported: version.V1_0,
	}
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)
	require.NoError(t, agg.OpenFolder())

	// Read the root through replaceShortenedKeysInBranch with the writer flag off. The
	// function must still walk the branch and dereference the source refs against the
	// per-range account/storage files — otherwise the recovered state would be garbage.
	postRoot := readStateRoot(t, db)
	require.Equal(t, preRoot, postRoot,
		"read of v2.0 ref source in v2.1 world must dereference refs and recover the same root")
}

// TestUpgrade_NewMergesProduceNorefV21 verifies that with the schema at v2.1 and refs off,
// any newly-built commitment .kv files (including those produced by merges that consume v2.0
// ref source files) land as v2.1 with full-key payload.
func TestUpgrade_NewMergesProduceNorefV21(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)
	dirs := agg.Dirs()
	_ = db

	// Sanity: schema current version is v2.1 (the branch state we're stacking on top of).
	require.Equal(t, version.V2_1, statecfg.Schema.CommitmentDomain.FileVersion.DataKV.Current,
		"this test assumes the v2.1 schema bump is in effect")

	// Any commitment kv file produced by this default build must be tagged v2.1 and carry no
	// referenced keys (the writer-side config disables ref production for non-squeeze flows).
	files, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	require.NoError(t, err)
	var commitmentFiles []string
	for _, f := range files {
		if strings.Contains(f, "commitment") {
			commitmentFiles = append(commitmentFiles, f)
		}
	}
	require.NotEmpty(t, commitmentFiles, "expected at least one commitment kv file")

	for _, f := range commitmentFiles {
		require.Contains(t, f, "v2.1-",
			"new merges/builds must tag commitment kv files as v2.1, got %s", f)
		assertCommitmentFileIsNoref(t, f)
	}
}

func readStateRoot(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	// false → don't save; recomputes root by reading account values + commitment branches.
	// The branch read path goes through replaceShortenedKeysInBranch — that's what we're
	// validating handles both ref (v2.0) and noref (v2.1) sources.
	root, err := domains.ComputeCommitment(t.Context(), rwTx, false, 0, 0, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, root)
	return root
}

// assertCommitmentFileIsNoref opens a commitment .kv file and walks every branch value,
// asserting that all account / storage keys are at full length (20B / 52B). Any shorter key
// would be a shortened ref encoding — illegal in a noref file.
func assertCommitmentFileIsNoref(t *testing.T, path string) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()

	g := seg.NewReader(d.MakeGetter(), seg.CompressNone)
	g.Reset(0)
	var k, v []byte
	for g.HasNext() {
		k, _ = g.Next(k[:0])
		if !g.HasNext() {
			break
		}
		v, _ = g.Next(v[:0])
		if len(v) == 0 {
			continue
		}
		// The state key carries hex-trie state metadata, not a BranchData encoding — skip.
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			continue
		}
		_, err := commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			if isStorage {
				require.Equal(t, length.Addr+length.Hash, len(key),
					"noref file %s: storage key length %d (expected %d, shorter implies ref encoding)",
					path, len(key), length.Addr+length.Hash)
			} else {
				require.Equal(t, length.Addr, len(key),
					"noref file %s: account key length %d (expected %d, shorter implies ref encoding)",
					path, len(key), length.Addr)
			}
			return nil, nil
		})
		require.NoError(t, err)
	}
}
