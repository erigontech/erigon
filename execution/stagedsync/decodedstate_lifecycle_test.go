package stagedsync_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/decodedstate"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/vm"
)

func decodedHistoryRowCountInDB(t *testing.T, db kv.RoDB) int {
	t.Helper()

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.DecodedHistory)
	require.NoError(t, err)
	defer cursor.Close()

	count := 0
	for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
		require.NoError(t, err)
		count++
	}

	return count
}

func TestS_UNW_07_ExecutionStageUnwindRemovesDecodedStateFromReorgedBlocks(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.NotNil(t, firstDecodedLatestKey(t, m.DB), "fixture chain must populate decoded state before the reorg")

	syncCfg := m.Cfg().Sync
	execCfg := stagedsync.StageExecuteBlocksCfg(
		m.DB,
		m.Cfg().Prune,
		m.Cfg().BatchSize,
		m.ChainConfig,
		m.Engine,
		&vm.Config{},
		m.Notifications,
		m.Cfg().StateStream,
		false,
		m.Dirs,
		m.BlockReader,
		m.Cfg().Genesis,
		syncCfg,
		false,
		exec.NewBlockReadAheader(),
		m.Cfg().DecodedStateEnabled,
		decodedstate.Config{},
	)

	syncState := stagedsync.New(syncCfg, []*stagedsync.Stage{{ID: stages.Execution}}, nil, nil, log.New(), stages.ModeApplyingBlocks)
	stageState := &stagedsync.StageState{State: syncState, ID: stages.Execution, BlockNumber: chain.TopBlock.NumberU64()}
	unwindState := syncState.NewUnwindState(stages.Execution, 2, chain.TopBlock.NumberU64(), false, false)

	tx, err := m.DB.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	defer doms.Close()

	require.NoError(t, stagedsync.UnwindExecutionStage(unwindState, stageState, doms, tx, context.Background(), execCfg, log.New()))
	require.NoError(t, tx.Commit())

	require.Nil(
		t,
		firstDecodedLatestKey(t, m.DB),
		"execution-stage unwind must remove decoded entries from the reorged-away canonical blocks",
	)
}

func TestS_PRUNE_06_ExecutionStagePruneRemovesDecodedHistoryRows(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	before := decodedHistoryRowCountInDB(t, m.DB)
	require.Greater(t, before, 0, "fixture chain must create decoded history before prune runs")

	syncCfg := m.Cfg().Sync
	syncCfg.MaxReorgDepth = 0

	execCfg := stagedsync.StageExecuteBlocksCfg(
		m.DB,
		m.Cfg().Prune,
		m.Cfg().BatchSize,
		m.ChainConfig,
		m.Engine,
		&vm.Config{},
		m.Notifications,
		m.Cfg().StateStream,
		false,
		m.Dirs,
		m.BlockReader,
		m.Cfg().Genesis,
		syncCfg,
		false,
		exec.NewBlockReadAheader(),
		m.Cfg().DecodedStateEnabled,
		decodedstate.Config{},
	)

	syncState := stagedsync.New(syncCfg, []*stagedsync.Stage{{ID: stages.Execution}}, nil, nil, log.New(), stages.ModeApplyingBlocks)
	tx, err := m.DB.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	pruneState, err := syncState.PruneStageState(stages.Execution, chain.TopBlock.NumberU64(), tx, false)
	require.NoError(t, err)
	require.NoError(t, stagedsync.PruneExecutionStage(context.Background(), pruneState, tx, execCfg, time.Minute, log.New()))
	require.NoError(t, tx.Commit())

	after := decodedHistoryRowCountInDB(t, m.DB)
	require.Less(
		t,
		after,
		before,
		"execution-stage prune must remove decoded history rows produced during canonical decoded-state sync",
	)
}
