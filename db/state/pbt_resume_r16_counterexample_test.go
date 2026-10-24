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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/stretchr/testify/require"
)

func TestPBinR16ResumeRejectsFilesPastBase(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, 8)
	targetDB, targetAgg := testDbAndAggregatorv3(t, 8)
	initial := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(1), nonce: 1, balance: 1}}
	writePBinLeafStreamRange(t, baseDB, 0, 24, initial)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 3, unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	writePBinLeafStreamRange(t, targetDB, 0, 48, initial)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 6, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 24)
	var files []string
	require.NoError(t, filepath.WalkDir(targetAgg.Dirs().Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), "commitment.3-4.") {
			files = append(files, path)
		}
		return nil
	}))
	require.NotEmpty(t, files)
	for _, file := range files {
		destination := strings.Replace(file, "commitment.3-4.", "commitment.4-5.", 1)
		require.NoError(t, dir.RemoveAll(destination))
		require.NoError(t, os.Link(file, destination))
	}
	require.NoError(t, targetAgg.ReloadFiles())
	at := targetAgg.BeginFilesRo()
	var future bool
	for _, file := range at.Files(kv.CommitmentDomain) {
		if file.StartRootNum() == 32 && file.EndRootNum() == 40 {
			future = true
		}
	}
	at.Close()
	require.True(t, future, "the target must expose the extra binary file [32,40)")
	require.NotEmpty(t, pbinRangeRows(t, targetAgg, 32, 40), "the file past X must contain rows")
	_, err := state.NewPBinRangeWriterForResume(targetAgg, kv.CommitmentDomain, 24, 48)
	require.Error(t, err, "resume accepted binary file [32,40) past base X=24 because it is before Y=48; got %v", err)
}
