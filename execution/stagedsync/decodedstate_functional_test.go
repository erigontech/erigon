package stagedsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/node/ethconfig"
)

func firstDecodedLatestKey(t *testing.T, db kv.RoDB) []byte {
	t.Helper()

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.DecodedLatest)
	require.NoError(t, err)
	defer cursor.Close()

	k, _, err := cursor.First()
	require.NoError(t, err)
	return k
}

func createDecodedStateEnabledExecModule(t *testing.T) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack, []*blockgen.ChainPack) {
	t.Helper()

	m, chain, orphaned, err := rpcdaemontest.CreateTestExecModuleWithError(
		t,
		execmoduletester.WithEthConfig(func(cfg *ethconfig.Config) {
			cfg.DecodedStateEnabled = true
			cfg.DecodedStateFullMode = true
		}),
	)
	require.NoError(t, err)
	return m, chain, orphaned
}

func TestS_NODE_08_CanonicalSyncPopulatesDecodedStateDuringRealChainExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.GreaterOrEqual(t, chain.TopBlock.NumberU64(), uint64(5), "fixture chain must include contract writes beyond deployment")
	require.True(t, m.Cfg().DecodedStateEnabled, "fixture config must explicitly enable decoded state for the enabled serial-path scenario")

	require.NotNil(
		t,
		firstDecodedLatestKey(t, m.DB),
		"canonical staged sync must populate decoded state from real SSTORE/KECCAK writes during chain execution",
	)
}

func TestS_NODE_10_DisabledByDefaultRuntimeDoesNotPopulateDecodedState(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, chain, _ := rpcdaemontest.CreateTestExecModule(t)
	require.GreaterOrEqual(t, chain.TopBlock.NumberU64(), uint64(5), "fixture chain must include contract writes beyond deployment")
	require.False(t, m.Cfg().DecodedStateEnabled, "decoded-state support must be disabled by default in the real node config")

	require.Nil(
		t,
		firstDecodedLatestKey(t, m.DB),
		"when decoded.enabled is false, canonical sync must not install decoded hooks or populate decoded tables",
	)
}

func TestS_NODE_11_ParallelCanonicalSyncPopulatesDecodedStateDuringRealChainExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	prevParallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = true
	t.Cleanup(func() {
		dbg.Exec3Parallel = prevParallel
	})

	m, chain, _ := createDecodedStateEnabledExecModule(t)
	require.GreaterOrEqual(t, chain.TopBlock.NumberU64(), uint64(5), "fixture chain must include contract writes beyond deployment")
	require.True(t, m.Cfg().DecodedStateEnabled, "fixture config must explicitly enable decoded state for the enabled parallel-path scenario")

	require.NotNil(
		t,
		firstDecodedLatestKey(t, m.DB),
		"parallel canonical sync must flush decoded state collected from live SSTORE/KECCAK writes into the shared decoded tables",
	)
}
