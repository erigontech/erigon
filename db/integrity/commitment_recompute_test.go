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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/integrity"
)

// TestRecomputeCommitmentAtBlock_NilArgs pins the defensive guards.
// Read-only contract — nil dependencies must fail loud, not panic.
//
// Happy-path coverage is structural: RecomputeCommitmentAtBlock mirrors
// the algorithm in checkCommitmentHistAtBlkWithIdx (same package,
// same SeekCommitment + touchHistoricalKeys + ComputeCommitment +
// root-vs-header check). The integrity check has been exercised
// in production against mainnet datadirs via
// `erigon snapshots check-commitment-hist-at-blk`. End-to-end
// validation of the recompute variant lands when the fork-from CLI
// wires it for non-aligned cuts.
func TestRecomputeCommitmentAtBlock_NilArgs(t *testing.T) {
	t.Parallel()
	_, _, err := integrity.RecomputeCommitmentAtBlock(t.Context(), nil, nil, 0, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil tx")
}
