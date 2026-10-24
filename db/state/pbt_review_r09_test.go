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
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestPBinReviewR09RejectedEndCleansRanges(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	pattern := filepath.Join(agg.Dirs().Tmp, "pbin-range-*")
	before, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Empty(t, before)
	_, err = state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, math.MaxUint64)
	require.ErrorContains(t, err, "end txNum is too large")
	after, err := filepath.Glob(pattern)
	require.NoError(t, err)
	if len(after) != 0 {
		t.Errorf("rejected writer left %d range temp directories; want 0", len(after))
	}
}
