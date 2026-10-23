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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestStepAlignedEndRefusesListStartingOffBoundary(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	generateAccountsFile(t, dirs, []testFileRange{{0, 20}})
	generateStorageFile(t, dirs, []testFileRange{{0, 20}})
	generateCommitmentFile(t, dirs, []testFileRange{{0, 20}})
	generateCodeFile(t, dirs, []testFileRange{{4, 20}})
	require.NoError(t, agg.OpenFolder(db))
	at := agg.BeginFilesRo()
	require.Equal(t, uint64(40), at.d[kv.CodeDomain].files.StartTxNum())
	at.Close()
	_, err := agg.StepAlignedEnd(100)
	require.ErrorContains(t, err, "starts at txNum 40")
}
