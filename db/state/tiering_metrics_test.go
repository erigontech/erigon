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

func TestTierProbeCountsSplitCommitmentProbesByTier(t *testing.T) {
	_, agg := testAggWithThreeSteps(t)
	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, agg.demoteColdDir))

	before := ReadTierProbeCounts()
	at := agg.BeginFilesRo()
	defer at.Close()
	_, found, _, _, err := at.DebugGetLatestFromFiles(kv.CommitmentDomain, []byte("missing-key"), 0)
	require.NoError(t, err)
	require.False(t, found)
	_, _, _, _, err = at.DebugGetLatestFromFiles(kv.AccountsDomain, []byte("missing-key"), 0)
	require.NoError(t, err)
	after := ReadTierProbeCounts()

	require.Equal(t, TierProbeCounts{SSD: 2, HDD: 1, HDDLookups: 1, Lookups: 1}, after.Sub(before), "commitment only; every file probed for a missing key")
}
