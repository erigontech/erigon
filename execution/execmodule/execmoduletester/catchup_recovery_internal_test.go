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

package execmoduletester

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func TestCatchupRetriesAfterMissingBlockResult(t *testing.T) {
	for _, failCycle := range []int{1, 2} {
		t.Run(fmt.Sprintf("cycle_%d", failCycle), func(t *testing.T) {
			m := New(t, WithStepSize(16), WithLoopBlockLimit(8))
			chain, err := m.GenerateChain(32, nil)
			require.NoError(t, err)
			status, err := m.InsertBlocks(t.Context(), chain.Blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)

			// Inject the executor's missing-result error just before a batch would commit.
			missingResult := fmt.Errorf("%w: block result missing", rules.ErrInvalidBlock)
			finishCfg := stagedsync.StageFinishCfg()
			cycle := 0
			m.posStagedSync.MockExecFunc(stages.Finish, func(_ bool, s *stagedsync.StageState, _ stagedsync.Unwinder, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
				cycle++
				if cycle == failCycle {
					return missingResult
				}
				return stagedsync.FinishForward(s, tx, finishCfg)
			})

			head := chain.TopBlock
			result, err := m.UpdateForkChoice(t.Context(), head.Header())
			m.ExecModule.WaitIdle(t.Context())
			require.ErrorIs(t, err, missingResult, "forkchoice returned %s", result.Status)
			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				hash, err := rawdb.ReadCanonicalHash(tx, head.NumberU64())
				if err != nil {
					return err
				}
				if failCycle == 1 {
					require.Zero(t, hash, "the first cycle has not committed the proposal")
				} else {
					require.Equal(t, head.Hash(), hash, "the committed proposal must remain available for retry")
				}
				return nil
			}))

			result, err = m.UpdateForkChoice(t.Context(), head.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
			require.Equal(t, head.Hash(), result.LatestValidHash)
		})
	}
}
