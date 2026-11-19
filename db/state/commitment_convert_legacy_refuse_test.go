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
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestConvertCommitmentFiles_LegacyModesRefuseV3Files(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		toV3                     state.ConvertOpts
		domainPrefix, histPrefix string
	}{
		{"domain and history", state.ConvertOpts{TargetV3: true}, "v3.", "v3."},
		{"history only", state.ConvertOpts{TargetV3: true, NoDomain: true}, "v2.", "v3."},
		{"domain only", state.ConvertOpts{TargetV3: true, NoHistory: true}, "v3.", "v2."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(false))
			legacySchema := statecfg.Schema
			db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 10, 4)
			runOrchestrator(t, db, tc.toV3)
			dirs := agg.Dirs()
			require.NoError(t, dir.RemoveAll(filepath.Join(dirs.Snap, "backup")))
			requireCommitmentFilesPrefixed(t, dirs.SnapDomain, "*-commitment.*.kv", tc.domainPrefix)
			requireCommitmentFilesPrefixed(t, dirs.SnapHistory, "*-commitment.*.v", tc.histPrefix)

			statecfg.Schema = legacySchema
			statecfg.EnableHistoricalCommitment()
			require.NoError(t, state.MatchCommitmentSchemaToFiles(dirs))
			db, agg = reopenAggregator(t, db, agg, 10)
			require.NoError(t, agg.OpenFolder(db))

			before := hashFilesUnder(t, dirs.Snap)
			require.NotEmpty(t, before)
			for _, opts := range []state.ConvertOpts{{TargetSqueeze: true}, {TargetNibblesV2: true}} {
				require.ErrorContains(t, convertErr(t, db, opts), "apply to legacy commitment files only")
				require.Equal(t, before, hashFilesUnder(t, dirs.Snap))
				require.NoDirExists(t, filepath.Join(dirs.Snap, "backup"))
				require.NoDirExists(t, filepath.Join(dirs.Snap, "rebuild"))
			}
		})
	}
}

func requireCommitmentFilesPrefixed(t *testing.T, dirPath, glob, prefix string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dirPath, glob))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.Truef(t, strings.HasPrefix(filepath.Base(f), prefix), "%s is not %s", filepath.Base(f), prefix)
	}
}

func hashFilesUnder(t *testing.T, root string) map[string]string {
	t.Helper()
	sums := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sums[path] = fmt.Sprintf("%x", sha256.Sum256(data))
		return nil
	}))
	return sums
}
