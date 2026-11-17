package eth

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

func TestShutterBlockStateUsesExecutionProgress(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		headHash := common.Hash{1}
		require.NoError(t, rawdb.WriteHeaderNumber(tx, headHash, 42))
		require.NoError(t, rawdb.WriteHeadHeaderHash(tx, headHash))
		require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 40))
		_, err := rawdb.IncrementStateVersion(tx)
		return err
	}))
	backend := &Ethereum{chainDB: db}
	state, err := backend.shutterBlockState(t.Context())
	require.NoError(t, err)
	require.Equal(t, shutter.BlockState{BlockNum: 40, StateVersionID: 1}, state)
}

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0o750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := range 3 {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0o750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := range 2 {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0o750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := range 2 {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0o750)
				require.NoError(t, err)
				//t.Log("OK")
			}
		}
	}
	list, err := os.ReadDir(rootName)
	require.NoError(t, err)

	require.Len(t, list, 3)

	err = RemoveContents(rootName)
	require.NoError(t, err)

	list, err = os.ReadDir(rootName)
	require.NoError(t, err)

	require.Empty(t, list)
}

// MCP serves only the namespaces --http.api enables: it must not force admin, debug
// or trace on, and an empty list gives it no namespace-backed RPC methods.
func TestMCPNamespacesFollowHTTPAPI(t *testing.T) {
	for _, tc := range []struct {
		name string
		api  []string
		want []string
	}{
		{"eth only", []string{"eth"}, []string{"eth"}},
		{"debug enabled", []string{"eth", "debug"}, []string{"debug", "eth"}},
		{"empty", nil, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := httpcfg.HttpCfg{API: tc.api}
			logger := log.New()
			apis := jsonrpc.APIList(nil, nil, nil, nil, nil, nil, freezeblocks.NewBlockReader(nil), &cfg, nil, logger, nil, nil)
			srv, err := newMCPRPCServer(apis, &cfg, logger)
			require.NoError(t, err)
			defer srv.Stop()
			client := rpc.DialInProc(srv, logger)
			defer client.Close()

			modules, err := client.SupportedModules()
			require.NoError(t, err)
			got := slices.Sorted(maps.Keys(modules))
			got = slices.DeleteFunc(got, func(ns string) bool { return ns == "rpc" })
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSetUpBlockReaderRebuildsMissedAccessor(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	stateDB := temporaltest.NewTestDB(t, dirs)
	tx, sd := temporaltest.NewTestTxSD(t, stateDB)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, []byte{1}, []byte{1}, config3.DefaultStepSize, nil))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	require.NoError(t, stateDB.(*temporal.DB).BuildFiles(2*config3.DefaultStepSize, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))
	stateDB.Close()
	accessors, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-accounts.0-1.bt"))
	require.NoError(t, err)
	require.Len(t, accessors, 1)
	require.NoError(t, os.Remove(accessors[0]))

	chainDB := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, chainDB.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Snapshots, 1)
	}))
	cfg := ethconfig.Defaults
	_, _, blockSnapshots, temporalDB, err := SetUpBlockReader(t.Context(), chainDB, dirs, &cfg, &chain.Config{ChainName: networkname.Test}, log.New(), nil)
	require.NoError(t, err)
	t.Cleanup(blockSnapshots.Close)
	t.Cleanup(temporalDB.Close)
	require.FileExists(t, accessors[0])
}
