package stagedsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/decodedstate"
	"github.com/erigontech/erigon/execution/stagedsync"
)

func decodedLatestBlockInDB(t *testing.T, db kv.TemporalRoDB) uint64 {
	t.Helper()

	tx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	latestBlock, err := decodedstate.LatestBlockTx(tx)
	require.NoError(t, err)
	return latestBlock
}

func decodedDomainProgressInDB(t *testing.T, db kv.TemporalRoDB) uint64 {
	t.Helper()

	tx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	return tx.Debug().DomainProgress(kv.DecodedStorageDomain)
}

func TestS_NODE_15_DecodedStateResetClearsFlatAndDomainData(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.NotNil(t, firstDecodedLatestKey(t, m.DB), "fixture chain must populate decoded state before reset")
	require.Greater(t, decodedLatestBlockInDB(t, m.DB), uint64(0))
	require.Greater(t, decodedDomainProgressInDB(t, m.DB), uint64(0))
	require.GreaterOrEqual(t, chain.TopBlock.NumberU64(), uint64(5))

	require.NoError(t, stagedsync.StageDecodedStateReset(context.Background(), m.DB))

	require.Nil(t, firstDecodedLatestKey(t, m.DB), "decoded latest rows must be removed by decoded reset")
	require.Zero(t, decodedLatestBlockInDB(t, m.DB), "decoded latest-block marker must be cleared by decoded reset")
	require.Zero(t, decodedDomainProgressInDB(t, m.DB), "decoded temporal-domain progress must be cleared by decoded reset")
	require.Zero(t, decodedHistoryRowCountInDB(t, m.DB), "decoded history rows must be removed by decoded reset")
}

func TestS_NODE_16_DecodedStateReplayRebuildsFromExecutionHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.NoError(t, stagedsync.StageDecodedStateReset(context.Background(), m.DB))

	cfg := stagedsync.StageDecodedStateCfg(m.DB, m.Dirs, m.BlockReader, m.ChainConfig, m.Engine, m.Cfg().Genesis, m.Cfg().Sync, decodedstate.Config{
		Enabled:  true,
		FullMode: true,
	})
	require.NoError(t, stagedsync.SpawnDecodedState(cfg, context.Background(), m.Log))

	require.NotNil(t, firstDecodedLatestKey(t, m.DB), "decoded replay must repopulate latest decoded rows from canonical execution history")
	require.Equal(t, chain.TopBlock.NumberU64(), decodedLatestBlockInDB(t, m.DB), "decoded replay must advance latest-block marker to execution progress")
	require.Greater(t, decodedDomainProgressInDB(t, m.DB), uint64(0), "decoded replay must repopulate the decoded temporal domain")
}

func TestS_NODE_17_DecodedStateReplayResumesFromLatestBlockMarker(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.NoError(t, stagedsync.StageDecodedStateReset(context.Background(), m.DB))

	cfg := stagedsync.StageDecodedStateCfg(m.DB, m.Dirs, m.BlockReader, m.ChainConfig, m.Engine, m.Cfg().Genesis, m.Cfg().Sync, decodedstate.Config{
		Enabled:  true,
		FullMode: true,
	})
	cfg.ToBlock = 2
	require.NoError(t, stagedsync.SpawnDecodedState(cfg, context.Background(), m.Log))
	require.Equal(t, uint64(2), decodedLatestBlockInDB(t, m.DB), "partial replay must record the last rebuilt block")

	cfg.ToBlock = 0
	require.NoError(t, stagedsync.SpawnDecodedState(cfg, context.Background(), m.Log))
	require.Equal(t, chain.TopBlock.NumberU64(), decodedLatestBlockInDB(t, m.DB), "second replay must resume from the decoded latest-block marker and reach execution tip")
	require.NotNil(t, firstDecodedLatestKey(t, m.DB), "decoded replay resume must preserve rebuilt decoded rows")
}

func TestS_NODE_19_DecodedStateReplayHonorsExplicitStartBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.NoError(t, stagedsync.StageDecodedStateReset(context.Background(), m.DB))

	cfg := stagedsync.StageDecodedStateCfg(m.DB, m.Dirs, m.BlockReader, m.ChainConfig, m.Engine, m.Cfg().Genesis, m.Cfg().Sync, decodedstate.Config{
		Enabled:  true,
		FullMode: true,
	})
	cfg.StartBlock = chain.TopBlock.NumberU64() - 1
	require.NoError(t, stagedsync.SpawnDecodedState(cfg, context.Background(), m.Log))
	tailRows := decodedHistoryRowCountInDB(t, m.DB)

	require.Equal(t, chain.TopBlock.NumberU64(), decodedLatestBlockInDB(t, m.DB), "explicit start block must allow replay to begin from a safe later boundary")

	require.NoError(t, stagedsync.StageDecodedStateReset(context.Background(), m.DB))
	cfg.StartBlock = 0
	require.NoError(t, stagedsync.SpawnDecodedState(cfg, context.Background(), m.Log))
	fullRows := decodedHistoryRowCountInDB(t, m.DB)

	require.GreaterOrEqual(t, fullRows, tailRows, "full replay must include at least as many decoded history rows as a tail-only replay started from decoded.start-block")
}
