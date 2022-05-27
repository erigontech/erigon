package core

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/stretchr/testify/require"
)

func TestDefaultBSCGenesisBlock(t *testing.T) {
	db := memdb.NewTestDB(t)
	check := func(network string) {
		genesis := DefaultGenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, block, err := WriteGenesisBlock(tx, genesis, nil, nil)
		require.NoError(t, err)
		expect := params.GenesisHashByChainName(network)
		require.NotNil(t, expect, network)
		require.Equal(t, block.Hash().Bytes(), expect.Bytes(), network)
	}
	for _, network := range networkname.All {
		check(network)
	}
}

func TestCommitGenesisIdempotency(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	genesis := DefaultGenesisBlockByChainName(networkname.MainnetChainName)
	_, _, err := WriteGenesisBlock(tx, genesis, nil, nil)
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = WriteGenesisBlock(tx, genesis, nil, nil)
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}
