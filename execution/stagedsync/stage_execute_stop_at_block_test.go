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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

// With execution already at the stop block, the Execution stage must return
// before it touches the (nil) shared domains, although Senders is ahead.
func TestExecutionStageStopsAtExecStopAtBlock(t *testing.T) {
	const stopAt, sendersProgress = 5, 10
	exec := ExecuteBlockCfg{syncCfg: ethconfig.Sync{ExecStopAtBlock: stopAt}}

	pipelines := map[string][]*Stage{
		"DefaultStages":  DefaultStages(t.Context(), SnapshotsCfg{}, HeadersCfg{}, BlockHashesCfg{}, BodiesCfg{}, SendersCfg{}, exec, TxLookupCfg{}, FinishCfg{}),
		"PipelineStages": PipelineStages(t.Context(), SnapshotsCfg{}, BlockHashesCfg{}, SendersCfg{}, exec, TxLookupCfg{}, FinishCfg{}),
	}
	for name, stageList := range pipelines {
		t.Run(name, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, stages.SaveStageProgress(tx, stages.Senders, sendersProgress))

			var execStage *Stage
			for _, s := range stageList {
				if s.ID == stages.Execution {
					execStage = s
				}
			}
			require.NotNil(t, execStage)

			s := &StageState{ID: stages.Execution, BlockNumber: stopAt + 1}
			require.NotPanics(t, func() {
				require.NoError(t, execStage.Forward(false, s, nil, nil, tx, log.New()))
			})
		})
	}
}
