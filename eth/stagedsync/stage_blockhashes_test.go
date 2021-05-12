package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockHashStage(t *testing.T) {
	origin, headers := generateFakeBlocks(1, 4)

	db := ethdb.NewMemKV()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	var dbtx ethdb.RwTx
	if hasTx, ok := tx.(ethdb.HasTx); ok {
		dbtx = hasTx.Tx().(ethdb.RwTx)
	}

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(tx, origin.Hash(), 0)
	if err := rawdb.WriteTd(tx, origin.Hash(), 0, origin.Difficulty); err != nil {
		panic(err)
	}
	rawdb.WriteHeader(tx, origin)
	if err := rawdb.WriteHeadHeaderHash(tx, origin.Hash()); err != nil {
		t.Fatalf("failed to write head header hash: %v", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, origin.Number.Uint64()); err != nil {
		t.Fatalf("setting headers progress: %v", err)
	}
	if err := rawdb.WriteCanonicalHash(tx, origin.Hash(), 0); err != nil {
		t.Fatalf("writing canonical hash: %v", err)
	}

	if _, _, _, err := InsertHeaderChain("logPrefix", ethdb.WrapIntoTxDB(tx), headers, 0); err != nil {
		t.Errorf("inserting header chain: %v", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, headers[len(headers)-1].Number.Uint64()); err != nil {
		t.Fatalf("setting headers progress: %v", err)
	}
	blockHashCfg := StageBlockHashesCfg(db, "")
	err = SpawnBlockHashStage(&StageState{Stage: stages.BlockHashes}, dbtx, blockHashCfg, nil)
	assert.NoError(t, err)
	for _, h := range headers {
		n := rawdb.ReadHeaderNumber(tx, h.Hash())
		assert.Equal(t, *n, h.Number.Uint64())
	}

}
