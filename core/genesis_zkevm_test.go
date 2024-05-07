package core_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestGenesisBlockHashesZkevm(t *testing.T) {
	check := func(network string) {
		db := memdb.NewTestDB(t)
		defer db.Close()
		genesis := core.GenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		logger := log.New()
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
