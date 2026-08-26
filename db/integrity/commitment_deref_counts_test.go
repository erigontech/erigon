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

package integrity

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
)

// TestCheckCommitmentKvDerefCounts pins that a file the referencing scan clears still reports the
// keys it walked. An all-zero tally is indistinguishable from having scanned nothing, so a run over
// a fully plain datadir would otherwise produce a summary that proves neither outcome.
func TestCheckCommitmentKvDerefCounts(t *testing.T) {
	t.Run("plain file reports what it walked", func(t *testing.T) {
		f := fakeVisibleFile{path: writeCommitmentKV(t, false), endTxNum: 20, version: version.V2_0}
		counts, err := checkCommitmentKvDeref(t.Context(), f, 10, true /* failFast */, log.New())
		require.NoError(t, err)
		require.Equal(t, uint64(1), counts.branchKeys)
		require.Equal(t, uint64(1), counts.plainAccounts)
		require.Zero(t, counts.referencedAccounts)
		require.Zero(t, counts.referencedStorages)
	})
}
