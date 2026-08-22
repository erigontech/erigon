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

package stagedsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

// TestResolveExecResumePoint covers the second exec cycle on a DB that already has
// commitment state. Exec-only mode (discardCommitment) never advances
// KeyCommitmentState, so after a first size-limited batch flushes flat state past
// the committed commitment boundary, SeekCommitment still reports the stale
// pre-run boundary. The resume must restart from Execution progress, not re-execute
// the already-advanced blocks over their own post-state. Normal (commitment) mode
// must keep the commitment boundary authoritative even when Execution ran ahead.
func TestResolveExecResumePoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 10_000)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// block b owns txNums [perBlock*b, perBlock*b+perBlock-1]; Max(b) == last txNum.
	const perBlock = uint64(10)
	for b := uint64(0); b <= 9; b++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, b, b*perBlock+perBlock-1))
	}

	const (
		commitmentBlock  = uint64(5)  // SeekCommitment boundary (last block with commitment)
		commitmentTxNum  = uint64(59) // its committed txNum, as SeekCommitment would report
		execProgress     = uint64(8)  // Execution progress after a prior flat-state batch
		execProgressTxNu = uint64(89) // Max(execProgress) = 8*10 + 9
	)

	t.Run("discard resumes from exec progress when it ran ahead", func(t *testing.T) {
		txNum, blockNum, err := resolveExecResumePoint(ctx, rawdbv3.TxNums, tx, true, execProgress, commitmentTxNum, commitmentBlock)
		require.NoError(t, err)
		require.Equal(t, execProgress, blockNum, "must resume from Execution progress, not the stale commitment boundary")
		require.Equal(t, execProgressTxNu, txNum, "resume txNum must be Max(execProgress)")
	})

	t.Run("discard keeps commitment boundary when exec has not advanced past it", func(t *testing.T) {
		txNum, blockNum, err := resolveExecResumePoint(ctx, rawdbv3.TxNums, tx, true, commitmentBlock, commitmentTxNum, commitmentBlock)
		require.NoError(t, err)
		require.Equal(t, commitmentBlock, blockNum)
		require.Equal(t, commitmentTxNum, txNum)
	})

	t.Run("commitment mode keeps the commitment boundary even when exec ran ahead", func(t *testing.T) {
		txNum, blockNum, err := resolveExecResumePoint(ctx, rawdbv3.TxNums, tx, false, execProgress, commitmentTxNum, commitmentBlock)
		require.NoError(t, err)
		require.Equal(t, commitmentBlock, blockNum, "commitment boundary is authoritative outside exec-only mode")
		require.Equal(t, commitmentTxNum, txNum)
	})
}
