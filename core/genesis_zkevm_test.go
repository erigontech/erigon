package core_test

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/params"
	"github.com/stretchr/testify/require"
)

func TestGenesisBlockHashesZkevm(t *testing.T) {
	logger := log.New()
	_, db, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	// defer db.Close()
	check := func(network string) {
		// db := memdb.NewTestDB(t)
		// defer db.Close()
		genesis := core.GenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, block, err := core.WriteGenesisBlock(tx, genesis, nil, "/tmp/"+network, logger)
		require.NoError(t, err)
		expect := params.GenesisHashByChainName(network)
		require.NotNil(t, expect, network)
		require.Equal(t, expect.Hex(), block.Hash().Hex(), network)
	}
	for _, network := range networkname.Zkevm {
		t.Run(network, func(t *testing.T) {
			check(network)
		})
	}
}

func TestCommitGenesisIdempotency2(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	genesis := core.GenesisBlockByChainName(networkname.HermezMainnetChainName)
	logger := log.New()
	_, _, err := core.WriteGenesisBlock(tx, genesis, nil, "8", logger)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, "9", logger)
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}
