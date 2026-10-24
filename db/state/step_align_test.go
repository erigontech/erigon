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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestAlignFilesToStepSize(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)
	const newStepSize = 10 * stepSize
	db, agg := testDbAndAggregatorv3(t, stepSize)
	keys := mkAddrs(0x10, 16)
	writeStepsKeys(t, db, agg, keys, 0, 23)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 23, unboundedFinalityCtx, true))
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

	end, _, err := agg.AlignFilesToStepSize(t.Context(), newStepSize)
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

func TestAlignFilesToStepSizeRefusesFrozenCommitment(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 10)
	require.NoError(t, state.WriteErigonDBSettings(agg.Dirs(), &state.ErigonDBSettings{StepSize: 10, StepsInFrozenFile: 64}))
	require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, 40))
	_, _, err := agg.AlignFilesToStepSize(t.Context(), 100)
	require.ErrorContains(t, err, "commitment domain is frozen")
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
