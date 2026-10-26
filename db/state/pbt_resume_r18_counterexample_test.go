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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/stretchr/testify/require"
)

func TestPBinR18ResumeRejectsCopiedFilesPastBase(t *testing.T) {
	for _, baseTx := range []uint64{23, 24} {
		t.Run(fmt.Sprint(baseTx), func(t *testing.T) {
			fromStep := baseTx / 8
			baseRange := fmt.Sprintf("commitment.%d-%d.", fromStep, fromStep+1)
			futureRange := fmt.Sprintf("commitment.%d-%d.", fromStep+1, fromStep+2)
			futureStart, futureEnd := (fromStep+1)*8, (fromStep+2)*8
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
			convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, baseTx)
			var files []string
			require.NoError(t, filepath.WalkDir(targetAgg.Dirs().Snap, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() && strings.Contains(entry.Name(), baseRange) {
					files = append(files, path)
				}
				return nil
			}))
			require.NotEmpty(t, files)
			for _, file := range files {
				destination := strings.Replace(file, baseRange, futureRange, 1)
				require.NoError(t, dir.RemoveAll(destination))
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(destination, data, 0o644))
			}
			require.NoError(t, targetAgg.ReloadFiles())
			at := targetAgg.BeginFilesRo()
			var future bool
			for _, file := range at.Files(kv.CommitmentDomain) {
				if file.StartRootNum() == futureStart && file.EndRootNum() == futureEnd {
					future = true
				}
			}
			at.Close()
			require.True(t, future, "the target must expose the extra binary file [%d,%d)", futureStart, futureEnd)
			require.NotEmpty(t, pbinRangeRows(t, targetAgg, futureStart, futureEnd), "the file past X must contain rows")
			_, err := state.NewPBinRangeWriterForResume(targetAgg, kv.CommitmentDomain, baseTx, 48)
			require.Error(t, err, "resume accepted copied binary file [%d,%d) past base X=%d; got %v", futureStart, futureEnd, baseTx, err)
		})
	}
}
