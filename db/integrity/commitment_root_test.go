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

package integrity_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
)

func TestCheckCommitmentRootFailsWithoutVisibleCommitmentFile(t *testing.T) {
	testCheckCommitmentRootFailsOnAGap(t, 2, "-commitment.")
}

func TestCheckCommitmentRootFailsOnAGapAfterTheFirstFiles(t *testing.T) {
	testCheckCommitmentRootFailsOnAGap(t, 7, "-commitment.4-6.")
}

func TestAssertFilesGapPassesOnFilesWithoutAGap(t *testing.T) {
	db, _, _ := buildCommitmentFiles(t, 7)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, state.AggTx(tx).AssertFilesGap())
}

func buildCommitmentFiles(t *testing.T, steps uint64) (kv.TemporalRwDB, *state.Aggregator, datadir.Dirs) {
	const stepSize = uint64(10)
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(stepSize))
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	writeAndBuild(t, t.Context(), db, agg, steps*stepSize)
	return db, agg, dirs
}

func testCheckCommitmentRootFailsOnAGap(t *testing.T, steps uint64, removed string) {
	db, agg, dirs := buildCommitmentFiles(t, steps)
	agg.CloseFilesNoReopen()
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	var n int
	for _, e := range entries {
		if strings.Contains(e.Name(), removed) {
			require.NoError(t, dir.RemoveFile(filepath.Join(dirs.SnapDomain, e.Name())))
			n++
		}
	}
	require.NotZero(t, n, "no %s file to remove", removed)
	require.NoError(t, agg.ReloadFiles())

	err = integrity.CheckCommitmentRoot(t.Context(), db, nil, true, log.New())
	require.ErrorIs(t, err, integrity.ErrIntegrity)
	require.ErrorContains(t, err, "a gap in files")
}

func TestCheckCommitmentRootPassesOnEmptyDatadir(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	require.NoError(t, integrity.CheckCommitmentRoot(t.Context(), db, nil, true, log.New()))
}
