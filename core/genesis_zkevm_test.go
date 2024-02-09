//go:build notzkevm
// +build notzkevm

package core_test

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenesisBlockHashesZkevm(t *testing.T) {
	db := memdb.NewTestDB(t)
	check := func(network string) {
		genesis := core.GenesisBlockByChainName(network)
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, block, err := core.WriteGenesisBlock(tx, genesis, nil, "")
		require.NoError(t, err)
		expect := params.GenesisHashByChainName(network)
		require.NotNil(t, expect, network)
		require.Equal(t, block.Hash().Bytes(), expect.Bytes(), network)
	}
	for _, network := range networkname.Zkevm {
		check(network)
	}
}

func TestCommitGenesisIdempotency2(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	genesis := core.GenesisBlockByChainName(networkname.HermezMainnetChainName)
	_, _, err := core.WriteGenesisBlock(tx, genesis, nil, "8")
	require.NoError(t, err)
	seq, err := tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, _, err = core.WriteGenesisBlock(tx, genesis, nil, "9")
	require.NoError(t, err)
	seq, err = tx.ReadSequence(kv.EthTx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}
