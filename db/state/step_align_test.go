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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestAlignFilesToStepSize(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		merge                 bool
		factor                uint64
		refsAtBuild, refsOnly bool
	}{
		{"merged files", true, 10, false, false},
		{"one file per step", false, 5, false, false},
		{"referenced commitment inputs", true, 10, true, false},
		{"references on", false, 5, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAlignFilesToStepSize(t, tc.merge, tc.factor, tc.refsAtBuild, tc.refsOnly)
		})
	}
}

func testAlignFilesToStepSize(t *testing.T, merge bool, factor uint64, refsAtBuild, refsAtAlign bool) {
	const stepSize = uint64(10)
	newStepSize := factor * stepSize
	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, refsAtBuild)
	keys := mkAddrs(0x10, 16)
	writeStepsKeys(t, db, agg, keys, 0, 23)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 23, unboundedFinalityCtx, merge))
	agg.WaitForFiles()
	dirs := agg.Dirs()

	const wantEnd = 20 * stepSize
	commitmentKeys := [][]byte{commitment.KeyCommitmentState}
	commitmentFiles, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	for _, path := range commitmentFiles {
		forEachCommitmentBranch(t, path, func(prefix, _ []byte) {
			commitmentKeys = append(commitmentKeys, bytes.Clone(prefix))
		})
	}
	before := readAlignedState(t, db, agg, keys, commitmentKeys, wantEnd)

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, refsAtAlign)
	end, err := agg.AlignFilesToStepSize(t.Context(), newStepSize)
	require.NoError(t, err)
	require.Equal(t, wantEnd, end)
	require.Equal(t, before, readAlignedState(t, db, agg, keys, commitmentKeys, wantEnd))

	agg.Close()
	for _, root := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		entries, err := os.ReadDir(root)
		require.NoError(t, err)
		for _, entry := range entries {
			parsed, _, ok := snaptype.ParseFileName(root, entry.Name())
			if !ok || parsed.To*stepSize > end {
				continue
			}
			require.Zerof(t, parsed.From*stepSize%newStepSize, "%s starts off a %d-tx boundary", entry.Name(), newStepSize)
			require.Zerof(t, parsed.To*stepSize%newStepSize, "%s ends off a %d-tx boundary", entry.Name(), newStepSize)
		}
	}
}

func TestAlignFilesToStepSizeIntegratesMergesThatSucceeded(t *testing.T) {
	t.Parallel()
	const stepSize, factor = uint64(10), uint64(5)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	writeStepsKeys(t, db, agg, mkAddrs(0x10, 16), 0, 23)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 23, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	dirs := agg.Dirs()
	inputs, err := filepath.Glob(filepath.Join(dirs.SnapIdx, "*-logaddrs.0-1.ef"))
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	blocked := strings.Replace(inputs[0], ".0-1.ef", ".0-5.ef", 1)
	require.NoError(t, os.MkdirAll(filepath.Join(blocked, "x"), 0o755))

	_, err = agg.AlignFilesToStepSize(t.Context(), factor*stepSize)
	require.Error(t, err)
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	for _, entry := range entries {
		parsed, _, ok := snaptype.ParseFileName(dirs.SnapDomain, entry.Name())
		if !ok || parsed.TypeString != "accounts" || parsed.Ext != ".kv" || parsed.To > 20 {
			continue
		}
		require.Equalf(t, factor, parsed.To-parsed.From, "%s was not integrated", entry.Name())
	}
}

func TestAlignFilesToStepSizeIntegratesEachMergeOnCompletion(t *testing.T) {
	const stepSize, factor = uint64(10), uint64(5)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	writeStepsKeys(t, db, agg, mkAddrs(0x10, 16), 0, 23)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 23, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	dirs := agg.Dirs()
	aligned := func() (n int) {
		for _, dir := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx} {
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			for _, entry := range entries {
				parsed, _, ok := snaptype.ParseFileName(dir, entry.Name())
				if ok && slices.Contains([]string{".kv", ".v", ".ef"}, parsed.Ext) && parsed.To-parsed.From == factor {
					n++
				}
			}
		}
		return n
	}
	atFirstDelete := -1
	agg.OnFilesChange(func([]string) {}, func([]string) {
		if atFirstDelete < 0 {
			atFirstDelete = aligned()
		}
	})
	mergeWorkers := dbg.MergeWorkers
	dbg.MergeWorkers = 1 << 20
	defer func() { dbg.MergeWorkers = mergeWorkers }()

	_, err := agg.AlignFilesToStepSize(t.Context(), factor*stepSize)
	require.NoError(t, err)
	require.Less(t, atFirstDelete, aligned())
}

func readAlignedState(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, accounts, commitmentKeys [][]byte, end uint64) map[string][]byte {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := agg.BeginFilesRo()
	defer at.Close()
	out := make(map[string][]byte)
	read := func(domain kv.Domain, key []byte) {
		v, found, _, _, err := at.DebugGetLatestFromFiles(domain, key, end-1)
		require.NoError(t, err)
		if found {
			out[domain.String()+string(key)] = bytes.Clone(v)
		}
	}
	for _, key := range accounts {
		read(kv.AccountsDomain, key)
		for txNum := uint64(0); txNum < end; txNum += 7 {
			v, ok, err := at.HistorySeek(kv.AccountsDomain, key, txNum, tx)
			require.NoError(t, err)
			if ok {
				out[string(key)+string(rune(txNum))] = bytes.Clone(v)
			}
		}
	}
	for _, key := range commitmentKeys {
		read(kv.CommitmentDomain, key)
	}
	return out
}
