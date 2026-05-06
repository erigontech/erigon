package eth

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := 0; i < 3; i++ {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := 0; j < 2; j++ {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := 0; k < 2; k++ {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0750)
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

func TestCheckAndSetCommitmentHistoryFlag(t *testing.T) {
	cases := []struct {
		name       string
		dbHasKey   bool   // whether db already has the blocks key set
		dbBlocks   uint64 // value persisted in db (when dbHasKey true)
		cfgBlocks  uint64 // value in config
		wantErr    bool
		wantErrSub string // substring expected in the error message
	}{
		// Fresh datadir: no key persisted yet, anything is accepted.
		{name: "fresh: cfg=0", dbHasKey: false, cfgBlocks: 0, wantErr: false},
		{name: "fresh: cfg=2M", dbHasKey: false, cfgBlocks: 2_000_000, wantErr: false},

		// Equal: no change, no error.
		{name: "equal: cfg=db=2M", dbHasKey: true, dbBlocks: 2_000_000, cfgBlocks: 2_000_000, wantErr: false},
		{name: "equal: cfg=db=0", dbHasKey: true, dbBlocks: 0, cfgBlocks: 0, wantErr: false},

		// Shrink: smaller window than persisted. Allowed.
		{name: "shrink: 2M->1M", dbHasKey: true, dbBlocks: 2_000_000, cfgBlocks: 1_000_000, wantErr: false},
		{name: "shrink from unbounded: 0->1M", dbHasKey: true, dbBlocks: 0, cfgBlocks: 1_000_000, wantErr: false},

		// Expand: bigger window than persisted. Rejected.
		{name: "expand: 1M->2M", dbHasKey: true, dbBlocks: 1_000_000, cfgBlocks: 2_000_000, wantErr: true, wantErrSub: "cannot be widened"},
		{name: "expand to unbounded: 1M->0", dbHasKey: true, dbBlocks: 1_000_000, cfgBlocks: 0, wantErr: true, wantErrSub: "cannot be widened"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := memdb.NewTestDB(t, dbcfg.ChainDB)
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			// Pre-seed bool flag so the bool branch passes (we exercise the blocks branch).
			require.NoError(t, rawdb.WriteDBCommitmentHistoryEnabled(tx, true))
			if tc.dbHasKey {
				require.NoError(t, rawdb.WriteDBCommitmentHistoryBlocks(tx, tc.dbBlocks))
			}

			cfg := &ethconfig.Config{}
			cfg.KeepExecutionProofs = true
			cfg.KeepExecutionProofsBlocks = tc.cfgBlocks

			// Use a tmp dir for the dirs.Chaindata reference (only used in error messages).
			dirs := datadir.New(t.TempDir())

			err = checkAndSetCommitmentHistoryFlag(tx, log.New(), dirs, cfg)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrSub != "" {
					require.Contains(t, err.Error(), tc.wantErrSub)
				}
				return
			}
			require.NoError(t, err)

			// Verify persistence: after a successful call, db should match cfg.
			got, _, err := rawdb.ReadDBCommitmentHistoryBlocks(tx)
			require.NoError(t, err)
			require.Equal(t, tc.cfgBlocks, got)
		})
	}
}
